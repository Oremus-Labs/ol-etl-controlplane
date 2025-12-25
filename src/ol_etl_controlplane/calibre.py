from __future__ import annotations

import json

import httpx

from ol_rag_pipeline_core.calibre import CalibreExportInput, CalibreExporter
from ol_rag_pipeline_core.models import Document, DocumentLink
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config


def _calibre_import(  # noqa: PLR0913
    *,
    logger,
    importer_enabled: bool,
    importer_url: str | None,
    importer_api_key: str | None,
    importer_timeout_s: float,
    bundle_prefix: str,
    document_id: str,
) -> None:
    if not importer_enabled:
        return
    if not importer_url:
        logger.warning("Calibre import skipped: importer_url missing document_id=%s", document_id)
        return

    url = importer_url.rstrip("/") + "/api/v1/import"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if importer_api_key:
        headers["Authorization"] = f"Bearer {importer_api_key}"

    try:
        with httpx.Client(timeout=importer_timeout_s, headers=headers, follow_redirects=True) as client:
            r = client.post(url, content=json.dumps({"prefix": bundle_prefix}))
        if r.status_code != 200:
            logger.warning(
                "Calibre import failed: document_id=%s status=%s body=%s",
                document_id,
                r.status_code,
                (r.text or "")[:800],
            )
            return
        payload = r.json()
        if not payload.get("ok"):
            logger.warning(
                "Calibre import failed: document_id=%s payload=%s",
                document_id,
                str(payload)[:800],
            )
            return
        logger.info(
            "Calibre import OK: document_id=%s book_id=%s prefix=%s",
            document_id,
            payload.get("book_id"),
            bundle_prefix,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Calibre import failed: document_id=%s err=%s", document_id, str(e))


def export_calibre_bundle(  # noqa: PLR0913
    *,
    enabled: bool,
    logger,
    docs: DocumentRepository,
    files_repo: DocumentFileRepository,
    document_id: str,
    pipeline_version: str,
    doc: Document,
    source_uri: str | None,
    raw_bytes: bytes,
    raw_content_type: str | None,
    raw_filename: str | None,
    extracted_text: str,
    categories: list[str],
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    calibre_bucket: str,
    calibre_prefix: str,
    importer_enabled: bool = True,
    importer_url: str | None = None,
    importer_api_key: str | None = None,
    importer_timeout_s: float = 60.0,
) -> None:
    if not enabled:
        return
    try:
        calibre_s3 = S3Client(
            S3Config(
                endpoint=s3_endpoint,
                bucket=calibre_bucket,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
            )
        )
        exporter = CalibreExporter(calibre_s3=calibre_s3, calibre_prefix=calibre_prefix)
        res = exporter.export(
            CalibreExportInput(
                document=doc,
                pipeline_version=pipeline_version,
                source_uri=source_uri,
                extracted_text=extracted_text,
                raw_bytes=raw_bytes,
                raw_content_type=raw_content_type,
                raw_filename=raw_filename,
                categories=categories,
            )
        )
        files_repo.upsert_file(
            document_id=document_id,
            variant="calibre_opf",
            storage_uri=res.opf_uri,
            mime_type="application/oebps-package+xml",
        )
        files_repo.upsert_file(
            document_id=document_id,
            variant="calibre_markdown",
            storage_uri=res.markdown_uri,
            mime_type="text/markdown",
        )
        files_repo.upsert_file(
            document_id=document_id,
            variant="calibre_epub",
            storage_uri=res.epub_uri,
            mime_type="application/epub+zip",
        )
        files_repo.upsert_file(
            document_id=document_id,
            variant="calibre_original",
            storage_uri=res.original_uri,
            mime_type=raw_content_type,
        )
        prefix_uri = f"s3://{calibre_bucket}/{res.base_prefix}/"
        docs.add_link(
            DocumentLink(
                document_id=document_id,
                link_type="calibre_inbox_prefix",
                url=prefix_uri,
                label="Calibre import bundle (MinIO)",
            )
        )
        logger.info("Calibre export OK: document_id=%s prefix=%s", document_id, prefix_uri)
        _calibre_import(
            logger=logger,
            importer_enabled=importer_enabled,
            importer_url=importer_url,
            importer_api_key=importer_api_key,
            importer_timeout_s=importer_timeout_s,
            bundle_prefix=res.base_prefix,
            document_id=document_id,
        )
    except Exception as e:  # noqa: BLE001
        # Calibre export should never block core ETL/indexing.
        logger.warning("Calibre export failed: document_id=%s err=%s", document_id, str(e))
