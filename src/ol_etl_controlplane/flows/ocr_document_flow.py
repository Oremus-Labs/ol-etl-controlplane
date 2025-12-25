from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.ocr import (
    LlmServiceClient,
    OcrEngineSpec,
    OcrEnsembleConfig,
    OcrPageInput,
    OcrQualityGate,
    render_pdf_to_png_pages,
    run_ocr_ensemble,
)
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.extractions import Extraction, ExtractionRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.repositories.ocr import OcrPage, OcrRepository, OcrRun
from ol_rag_pipeline_core.repositories.review_queue import ReviewQueueRepository
from ol_rag_pipeline_core.routing import deterministic_ocr_run_id
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config, parse_s3_uri
from ol_rag_pipeline_core.util import sha256_bytes
from ol_rag_pipeline_core.validation import validate_extracted_text
from prefect import flow, get_run_logger

from ol_etl_controlplane.calibre import export_calibre_bundle
from ol_etl_controlplane.config import load_settings
from ol_etl_controlplane.flows.index_document_flow import index_document_flow


def _pg_dsn_from_env(settings) -> str:  # noqa: ANN001
    cfg = PostgresConfig(
        dsn=settings.pg_dsn,
        host=settings.postgres_host,
        port=settings.postgres_port,
        db=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    return cfg.build_dsn()


def _parse_engines(csv: str) -> list[OcrEngineSpec]:
    engines = [e.strip() for e in (csv or "").split(",") if e.strip()]
    if not engines:
        raise ValueError("OCR_ENGINES is empty")
    return [OcrEngineSpec(engine=e) for e in engines]


def _filename_from_s3_uri(uri: str) -> str | None:
    _, key = parse_s3_uri(uri)
    name = key.rstrip("/").split("/")[-1]
    return name or None


@flow(name="ocr_document_flow")
def ocr_document_flow(
    document_id: str,
    pipeline_version: str | None = None,
    source: str | None = None,
    event_id: str | None = None,
    event: dict | None = None,
) -> dict[str, object]:
    """
    Phase 6:
    - Render scanned PDF to page images
    - Run OCR ensemble per page via ol-llm-service (OpenAI-compatible endpoint)
    - Persist per-page consensus artifacts + merged consensus
    - Apply quality gates; low quality -> review_queue
    - If OK: mark extracted and proceed to Phase 5 indexing with page-aware chunking
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    correlation_id = event_id or str(uuid4())

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    dsn = _pg_dsn_from_env(settings)
    apply_migrations(dsn, schema="public")

    s3 = S3Client(
        S3Config(
            endpoint=settings.s3_endpoint,
            bucket=settings.s3_bucket,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
        )
    )

    llm = LlmServiceClient(base_url=settings.llm_service_url, api_key=settings.llm_service_api_key)
    engines = _parse_engines(settings.ocr_engines)
    ensemble_cfg = OcrEnsembleConfig(
        engines=engines,
        quality_gate=OcrQualityGate(
            min_chars_per_page=settings.ocr_min_chars_per_page,
            min_alpha_ratio=settings.ocr_min_alpha_ratio,
            min_printable_ratio=settings.ocr_min_printable_ratio,
        ),
    )

    with connect(dsn, schema="public") as conn:
        docs = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)
        ext_repo = ExtractionRepository(conn)
        ocr_repo = OcrRepository(conn)
        review_repo = ReviewQueueRepository(conn)

        doc = docs.get_document(document_id)
        if not doc:
            raise RuntimeError(f"Document not found: {document_id}")

        if not doc.is_scanned:
            raise RuntimeError("ocr_document_flow called for non-scanned doc (is_scanned=false)")

        raw = files_repo.get_file(document_id=document_id, variant="raw")
        if not raw:
            raise RuntimeError(f"Raw file not found for document: {document_id}")

        ocr_run_id = deterministic_ocr_run_id(pipeline_version=pv, document_id=document_id)
        existing = ocr_repo.get_ocr_run(ocr_run_id)
        if existing and existing.status in {"complete", "needs_review"}:
            existing_fp = (existing.metrics_json or {}).get("content_fingerprint")
            if existing_fp and doc.content_fingerprint and existing_fp == doc.content_fingerprint:
                logger.info(
                    "Already OCR'd (idempotent): document_id=%s status=%s",
                    document_id,
                    existing.status,
                )
                return {
                    "document_id": document_id,
                    "pipeline_version": pv,
                    "status": existing.status,
                    "idempotent": True,
                }

        # Mark as in progress.
        ocr_repo.upsert_ocr_run(
            OcrRun(
                ocr_run_id=ocr_run_id,
                document_id=document_id,
                pipeline_version=pv,
                engine="ocr-ensemble",
                status="running",
                metrics_json={
                    "started_at": datetime.now(UTC).isoformat(),
                    "correlation_id": correlation_id,
                    "source": source or doc.source,
                    "raw_uri": raw.storage_uri,
                    "content_type": doc.content_type or raw.mime_type,
                    "content_fingerprint": doc.content_fingerprint,
                    "event": event,
                    "engines": [e.engine for e in engines],
                },
            )
        )

        raw_bytes = s3.get_bytes_uri(raw.storage_uri)
        try:
            rendered = render_pdf_to_png_pages(
                raw_bytes,
                dpi=settings.ocr_pdf_dpi,
                max_pages=settings.ocr_pdf_max_pages,
            )
        except Exception as e:  # noqa: BLE001
            review_repo.ensure_open_item(
                document_id=document_id,
                pipeline_version=pv,
                reason="ocr_render_failed",
            )
            docs.set_processing_state(
                document_id=document_id,
                status="needs_review",
                is_scanned=True,
            )
            ocr_repo.set_run_status(
                ocr_run_id=ocr_run_id,
                status="needs_review",
                metrics_json={
                    "error": "render_failed",
                    "message": str(e),
                    "correlation_id": correlation_id,
                },
            )
            raise

        page_inputs = [
            OcrPageInput(page_number=p.page_number, png_bytes=p.png_bytes) for p in rendered
        ]
        ocr_result = run_ocr_ensemble(client=llm, pages=page_inputs, cfg=ensemble_cfg)

        base_prefix = f"library/ocr/{pv}/{doc.source}/{document_id}"

        # Persist per-page artifacts + ocr_pages rows.
        for page in ocr_result.pages:
            page_dir = f"{base_prefix}/pages/{page.page_number:04d}"

            # Store page image for manual audit.
            png_uri = s3.put_bytes(
                f"{page_dir}/page.png",
                rendered[page.page_number - 1].png_bytes,
                content_type="image/png",
            )

            engine_uris: dict[str, str] = {}
            for engine_name, text in page.engine_texts.items():
                engine_uris[engine_name] = s3.put_bytes(
                    f"{page_dir}/engine-{engine_name}.txt",
                    (text.strip() + "\n").encode("utf-8"),
                    content_type="text/plain; charset=utf-8",
                )

            consensus_uri = s3.put_bytes(
                f"{page_dir}/consensus.txt",
                (page.consensus_text.strip() + "\n").encode("utf-8"),
                content_type="text/plain; charset=utf-8",
            )

            quality_json = {
                "passed_gate": page.passed_gate,
                "page_png_uri": png_uri,
                "engine_text_uris": engine_uris,
                "quality_by_engine": page.quality_by_engine,
                "errors_by_engine": page.errors_by_engine,
                "consensus_meta": page.consensus_meta,
            }

            ocr_repo.upsert_ocr_page(
                OcrPage(
                    ocr_run_id=ocr_run_id,
                    page_number=page.page_number,
                    consensus_uri=consensus_uri,
                    quality_json=quality_json,
                )
            )

        merged_uri = s3.put_bytes(
            f"{base_prefix}/consensus.txt",
            ocr_result.merged_text.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
        )

        # Also store as extracted_text so Phase 5 can index it.
        extracted_uri = s3.put_bytes(
            f"library/artifacts/{pv}/{doc.source}/{document_id}/extracted.txt",
            ocr_result.merged_text.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
        )
        files_repo.upsert_file(
            document_id=document_id,
            variant="extracted_text",
            storage_uri=extracted_uri,
            sha256=sha256_bytes(ocr_result.merged_text.encode("utf-8")),
            bytes_size=len(ocr_result.merged_text.encode("utf-8")),
            mime_type="text/plain",
        )

        ext_repo.upsert_extraction(
            Extraction(
                document_id=document_id,
                pipeline_version=pv,
                extractor="ocr_consensus",
                extracted_uri=extracted_uri,
                metrics_json={
                    "correlation_id": correlation_id,
                    "source": source or doc.source,
                    "content_fingerprint": doc.content_fingerprint,
                    "merged_consensus_uri": merged_uri,
                    "engines": [e.engine for e in engines],
                    "page_count": len(ocr_result.pages),
                    "overall_passed": ocr_result.overall_passed,
                },
            )
        )

        export_calibre_bundle(
            enabled=settings.calibre_export_enabled,
            logger=logger,
            docs=docs,
            files_repo=files_repo,
            document_id=document_id,
            pipeline_version=pv,
            doc=doc,
            source_uri=doc.source_uri,
            raw_bytes=raw_bytes,
            raw_content_type=doc.content_type or raw.mime_type,
            raw_filename=_filename_from_s3_uri(raw.storage_uri),
            extracted_text=ocr_result.merged_text,
            categories=docs.list_categories(document_id),
            s3_endpoint=settings.s3_endpoint,
            s3_access_key=settings.s3_access_key or "",
            s3_secret_key=settings.s3_secret_key or "",
            calibre_bucket=settings.calibre_s3_bucket,
            calibre_prefix=settings.calibre_s3_prefix,
            importer_enabled=settings.calibre_import_enabled,
            importer_url=settings.calibre_importer_url,
            importer_api_key=settings.calibre_importer_api_key,
            importer_timeout_s=settings.calibre_import_timeout_s,
        )

        # Validate merged text (same heuristics as Phase 4) + page gate.
        issues = validate_extracted_text(
            text=ocr_result.merged_text,
            content_type=doc.content_type or raw.mime_type,
            min_chars=settings.extract_min_chars,
            min_alpha_ratio=settings.extract_min_alpha_ratio,
        )

        if not ocr_result.overall_passed or issues:
            review_repo.ensure_open_item(
                document_id=document_id,
                pipeline_version=pv,
                reason="ocr_low_quality",
            )
            for issue in issues:
                review_repo.ensure_open_item(
                    document_id=document_id,
                    pipeline_version=pv,
                    reason=issue.code,
                )
            docs.set_processing_state(
                document_id=document_id,
                status="needs_review",
                is_scanned=True,
            )
            ocr_repo.set_run_status(
                ocr_run_id=ocr_run_id,
                status="needs_review",
                metrics_json={
                    "finished_at": datetime.now(UTC).isoformat(),
                    "correlation_id": correlation_id,
                    "merged_consensus_uri": merged_uri,
                    "extracted_uri": extracted_uri,
                    "page_count": len(ocr_result.pages),
                    "overall_passed": ocr_result.overall_passed,
                    "issues": [i.code for i in issues],
                },
            )
            return {
                "document_id": document_id,
                "pipeline_version": pv,
                "status": "needs_review",
                "issues": (
                    [i.code for i in issues]
                    + (["ocr_low_quality"] if not ocr_result.overall_passed else [])
                ),
            }

        docs.upsert_search_preview(document_id, ocr_result.merged_text[:5000])
        docs.set_processing_state(document_id=document_id, status="extracted", is_scanned=True)
        ocr_repo.set_run_status(
            ocr_run_id=ocr_run_id,
            status="complete",
            metrics_json={
                "finished_at": datetime.now(UTC).isoformat(),
                "correlation_id": correlation_id,
                "merged_consensus_uri": merged_uri,
                "extracted_uri": extracted_uri,
                "page_count": len(ocr_result.pages),
                "overall_passed": True,
                "engines": [e.engine for e in engines],
            },
        )

        logger.info("OCR complete: document_id=%s pages=%s", document_id, len(ocr_result.pages))
        index_result = index_document_flow(
            document_id=document_id,
            pipeline_version=pv,
            content_fingerprint=doc.content_fingerprint,
            source=source or doc.source,
            event_id=event_id,
            event=event,
        )
        return {
            "document_id": document_id,
            "pipeline_version": pv,
            "status": "indexed_ok",
            "ocr_pages": len(ocr_result.pages),
            "merged_consensus_uri": merged_uri,
            "extracted_uri": extracted_uri,
            "index": index_result,
        }
