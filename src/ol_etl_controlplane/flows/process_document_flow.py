from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from uuid import uuid4

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.extractors import extract_text
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.extractions import Extraction, ExtractionRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.repositories.ocr import OcrRepository, OcrRun
from ol_rag_pipeline_core.repositories.review_queue import ReviewQueueRepository
from ol_rag_pipeline_core.routing import deterministic_ocr_run_id
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config, parse_s3_uri
from ol_rag_pipeline_core.util import sha256_bytes
from ol_rag_pipeline_core.validation import validate_extracted_text
from prefect.deployments import run_deployment
from prefect import flow, get_run_logger

from ol_etl_controlplane.config import load_settings
from ol_etl_controlplane.calibre import export_calibre_bundle


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


def _filename_from_s3_uri(uri: str) -> str | None:
    _, key = parse_s3_uri(uri)
    name = key.rstrip("/").split("/")[-1]
    return name or None


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def _pick_index_deployment(document_id: str, fqns: list[str] | None) -> str:
    if not fqns:
        return "index_document_flow/index-document"
    if len(fqns) == 1:
        return fqns[0]
    digest = hashlib.sha256(document_id.encode("utf-8")).hexdigest()
    return fqns[int(digest, 16) % len(fqns)]


@flow(name="process_document_flow")
def process_document_flow(
    document_id: str,
    pipeline_version: str | None = None,
    content_fingerprint: str | None = None,
    source: str | None = None,
    event_id: str | None = None,
    event: dict | None = None,
    force: bool = False,
) -> dict[str, object]:
    """
    Phase 4:
    - Load raw bytes from MinIO
    - Extract/normalize born-digital text into MinIO + Postgres
    - Route scanned/binary documents to OCR (record queued OCR run)
    - Trigger indexing via Prefect deployment (pool-index)
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    correlation_id = event_id or str(uuid4())
    index_deployment_fqns = _parse_csv(settings.index_deployment_fqns)

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

    with connect(dsn, schema="public") as conn:
        docs = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)
        ext_repo = ExtractionRepository(conn)
        ocr_repo = OcrRepository(conn)
        review_repo = ReviewQueueRepository(conn)

        doc = docs.get_document(document_id)
        if not doc:
            raise RuntimeError(f"Document not found: {document_id}")
        prev_status = doc.status

        raw = files_repo.get_file(document_id=document_id, variant="raw")
        if not raw:
            raise RuntimeError(f"Raw file not found for document: {document_id}")

        filename = _filename_from_s3_uri(raw.storage_uri)
        effective_ct = doc.content_type or raw.mime_type
        categories = docs.list_categories(document_id)

        existing_ext = ext_repo.get_extraction(
            document_id=document_id,
            pipeline_version=pv,
            extractor="process_document",
        )
        existing_fp = (
            (existing_ext.metrics_json or {}).get("content_fingerprint") if existing_ext else None
        )
        if (
            existing_ext
            and doc.status in {"indexed_ok", "needs_review", "needs_ocr"}
            and doc.content_fingerprint
            and existing_fp
            and existing_fp == doc.content_fingerprint
            and not force
        ):
            logger.info("Already processed (idempotent): document_id=%s", document_id)
            return {
                "document_id": document_id,
                "pipeline_version": pv,
                "status": doc.status,
                "idempotent": True,
            }
        if force:
            logger.warning(
                "Force reprocess enabled: document_id=%s prev_status=%s",
                document_id,
                prev_status,
            )

        raw_bytes = s3.get_bytes_uri(raw.storage_uri)
        extracted = extract_text(data=raw_bytes, content_type=effective_ct, filename=filename)

        if extracted.is_scanned:
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
                raw_content_type=effective_ct,
                raw_filename=filename,
                extracted_text="(OCR pending)\n",
                categories=categories,
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
            docs.set_processing_state(document_id=document_id, status="needs_ocr", is_scanned=True)
            ocr_repo.upsert_ocr_run(
                OcrRun(
                    ocr_run_id=deterministic_ocr_run_id(
                        pipeline_version=pv,
                        document_id=document_id,
                    ),
                    document_id=document_id,
                    pipeline_version=pv,
                    engine="ocr-ensemble",
                    status="ready_for_ocr",
                    metrics_json={
                        "routed_at": datetime.now(UTC).isoformat(),
                        "correlation_id": correlation_id,
                        "source": source or doc.source,
                        "source_uri": doc.source_uri,
                        "content_fingerprint": doc.content_fingerprint,
                        "raw_uri": raw.storage_uri,
                        "raw_sha256": raw.sha256,
                        "raw_bytes": raw.bytes_size,
                        "content_type": effective_ct,
                        "next_step": "ocr",
                        "prefect_pool": "pool-ocr",
                        "event": event,
                        "metrics": extracted.metrics,
                    },
                )
            )
            logger.info("Routed to OCR: document_id=%s content_type=%s", document_id, effective_ct)
            return {
                "document_id": document_id,
                "pipeline_version": pv,
                "status": "needs_ocr",
                "routed": "ocr",
            }

        text_bytes = extracted.text.encode("utf-8")
        extracted_key = f"library/artifacts/{pv}/{doc.source}/{document_id}/extracted.txt"
        extracted_uri = s3.put_bytes(
            extracted_key,
            text_bytes,
            content_type="text/plain; charset=utf-8",
        )

        ext_repo.upsert_extraction(
            Extraction(
                document_id=document_id,
                pipeline_version=pv,
                extractor="process_document",
                extracted_uri=extracted_uri,
                metrics_json={
                    "correlation_id": correlation_id,
                    "content_fingerprint": doc.content_fingerprint,
                    "content_type": effective_ct,
                    "extractor": extracted.extractor,
                    **extracted.metrics,
                },
            )
        )

        files_repo.upsert_file(
            document_id=document_id,
            variant="extracted_text",
            storage_uri=extracted_uri,
            sha256=sha256_bytes(text_bytes),
            bytes_size=len(text_bytes),
            mime_type="text/plain",
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
            raw_content_type=effective_ct,
            raw_filename=filename,
            extracted_text=extracted.text,
            categories=categories,
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

        issues = validate_extracted_text(
            text=extracted.text,
            content_type=effective_ct,
            min_chars=settings.extract_min_chars,
            min_alpha_ratio=settings.extract_min_alpha_ratio,
        )

        docs.upsert_search_preview(document_id, extracted.text[:5000])
        if issues:
            for issue in issues:
                review_repo.ensure_open_item(
                    document_id=document_id,
                    pipeline_version=pv,
                    reason=issue.code,
                )
            docs.set_processing_state(
                document_id=document_id,
                status="needs_review",
                is_scanned=False,
            )
            logger.warning(
                "Extracted but needs review: document_id=%s issues=%s",
                document_id,
                [i.code for i in issues],
            )
            return {
                "document_id": document_id,
                "pipeline_version": pv,
                "status": "needs_review",
                "issues": [i.code for i in issues],
            }

        # Avoid downgrading a previously-indexed document on reruns. If indexing fails later,
        # keep the last known-good `indexed_ok` state instead of regressing to `extracted`.
        if prev_status != "indexed_ok":
            docs.set_processing_state(document_id=document_id, status="extracted", is_scanned=False)

        logger.info("Extracted: document_id=%s chars=%s", document_id, len(extracted.text))

        # Do NOT run indexing inline: it bypasses pool-index and can overwhelm embeddings/Qdrant.
        # Instead, enqueue the index deployment (which runs on pool-index with bounded concurrency).
        fr = run_deployment(
            name=_pick_index_deployment(document_id, index_deployment_fqns),
            parameters={
                "document_id": document_id,
                "pipeline_version": pv,
                "content_fingerprint": doc.content_fingerprint,
                "source": source or doc.source,
                "event_id": event_id,
                "event": event,
            },
        )
        index_flow_run_id = getattr(fr, "id", fr)
        logger.info(
            "Enqueued index_document_flow: document_id=%s flow_run_id=%s",
            document_id,
            index_flow_run_id,
        )
        resolved = review_repo.resolve_open_items(
            document_id=document_id,
            pipeline_version=pv,
        )
        if resolved:
            logger.info("Resolved review_queue items: document_id=%s count=%s", document_id, resolved)
        return {
            "document_id": document_id,
            "pipeline_version": pv,
            "status": "extracted",
            "extracted_chars": len(extracted.text),
            "extracted_sha256": sha256_bytes(text_bytes),
            "index": {"enqueued": True, "flow_run_id": str(index_flow_run_id)},
        }
