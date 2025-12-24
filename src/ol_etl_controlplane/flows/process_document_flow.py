from __future__ import annotations

from datetime import UTC, datetime
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
from prefect import flow, get_run_logger

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


def _filename_from_s3_uri(uri: str) -> str | None:
    _, key = parse_s3_uri(uri)
    name = key.rstrip("/").split("/")[-1]
    return name or None


@flow(name="process_document_flow")
def process_document_flow(
    document_id: str,
    pipeline_version: str | None = None,
    content_fingerprint: str | None = None,
    source: str | None = None,
    event_id: str | None = None,
    event: dict | None = None,
) -> dict[str, object]:
    """
    Phase 4:
    - Load raw bytes from MinIO
    - Extract/normalize born-digital text into MinIO + Postgres
    - Route scanned/binary documents to OCR (record queued OCR run)
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

    with connect(dsn, schema="public") as conn:
        docs = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)
        ext_repo = ExtractionRepository(conn)
        ocr_repo = OcrRepository(conn)
        review_repo = ReviewQueueRepository(conn)

        doc = docs.get_document(document_id)
        if not doc:
            raise RuntimeError(f"Document not found: {document_id}")

        raw = files_repo.get_file(document_id=document_id, variant="raw")
        if not raw:
            raise RuntimeError(f"Raw file not found for document: {document_id}")

        filename = _filename_from_s3_uri(raw.storage_uri)
        effective_ct = doc.content_type or raw.mime_type

        existing_ext = ext_repo.get_extraction(
            document_id=document_id,
            pipeline_version=pv,
            extractor="process_document",
        )
        if (
            existing_ext
            and doc.content_fingerprint
            and doc.content_fingerprint == (content_fingerprint or "")
        ):
            logger.info("Already processed (idempotent): document_id=%s", document_id)
            return {
                "document_id": document_id,
                "pipeline_version": pv,
                "status": doc.status,
                "idempotent": True,
            }

        raw_bytes = s3.get_bytes_uri(raw.storage_uri)
        extracted = extract_text(data=raw_bytes, content_type=effective_ct, filename=filename)

        if extracted.is_scanned:
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
            docs.set_processing_state(document_id=document_id, status="needs_review", is_scanned=False)
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

        docs.set_processing_state(document_id=document_id, status="extracted", is_scanned=False)

        logger.info("Extracted: document_id=%s chars=%s", document_id, len(extracted.text))
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
            "status": index_result.get("status", "extracted"),
            "extracted_chars": len(extracted.text),
            "extracted_sha256": sha256_bytes(text_bytes),
            "index": index_result,
        }
