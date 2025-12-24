from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.models import Document
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from prefect import flow, get_run_logger

from ol_etl_controlplane.config import load_settings

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


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


def _extract_title_from_html(body: bytes) -> str | None:
    try:
        text = body[:65536].decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return None
    m = _TITLE_RE.search(text)
    if not m:
        return None
    title = re.sub(r"\s+", " ", m.group(1) or "").strip()
    return title or None


def _canonical_url_from_source(source: str, source_uri: str) -> str | None:
    if not source_uri:
        return None
    if source_uri.startswith(("http://", "https://")):
        return source_uri
    if source == "newadvent_zip" and source_uri.startswith("newadvent_zip://"):
        path = source_uri.removeprefix("newadvent_zip://")
        return "https://www.newadvent.org/" + path.lstrip("/")
    return source_uri


def _fallback_title_from_source(source: str, source_uri: str) -> str | None:
    if not source_uri:
        return None
    if source == "newadvent_zip" and source_uri.startswith("newadvent_zip://"):
        path = source_uri.removeprefix("newadvent_zip://")
        name = path.rstrip("/").split("/")[-1]
        return name or None
    if source == "nextcloud" and source_uri.startswith("nextcloud://"):
        name = source_uri.split("/")[-1]
        return name or None
    name = source_uri.rstrip("/").split("/")[-1]
    return name or None


@flow(name="metadata_backfill_flow")
def metadata_backfill_flow(
    *,
    max_docs: int = 5000,
    sources_csv: str | None = None,
    dry_run: bool = True,
) -> dict[str, object]:
    """
    Backfills missing `documents.title` and `documents.canonical_url` without re-running source
    syncs.

    This is a day-2 tool to improve UI/search metadata for already-ingested docs.
    """
    settings = load_settings()
    logger = get_run_logger()

    dsn = _pg_dsn_from_env(settings)
    apply_migrations(dsn, schema="public")

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    s3 = S3Client(
        S3Config(
            endpoint=settings.s3_endpoint,
            bucket=settings.s3_bucket,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
        )
    )

    source_filter = None
    if sources_csv:
        source_filter = {s.strip() for s in sources_csv.split(",") if s.strip()}

    actions: list[dict[str, Any]] = []
    updated = 0

    with connect(dsn, schema="public") as conn:
        docs_repo = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)

        rows = conn.execute(
            """
            select document_id
            from documents
            order by updated_at desc
            limit %s
            """,
            (max_docs,),
        ).fetchall()

        for (document_id,) in rows:
            doc = docs_repo.get_document(document_id)
            if not doc:
                continue
            if source_filter is not None and doc.source not in source_filter:
                continue

            new_canonical_url = doc.canonical_url
            new_title = doc.title

            if not new_canonical_url:
                new_canonical_url = _canonical_url_from_source(doc.source, doc.source_uri)

            if not new_title:
                if doc.source == "newadvent_zip":
                    raw = files_repo.get_file(document_id=document_id, variant="raw")
                    if raw:
                        raw_bytes = s3.get_bytes_uri(raw.storage_uri)
                        new_title = _extract_title_from_html(raw_bytes) or new_title
                new_title = new_title or _fallback_title_from_source(doc.source, doc.source_uri)

            if new_title == doc.title and new_canonical_url == doc.canonical_url:
                continue

            actions.append(
                {
                    "action": "update_document",
                    "document_id": document_id,
                    "source": doc.source,
                    "title_before": doc.title,
                    "title_after": new_title,
                    "canonical_url_before": doc.canonical_url,
                    "canonical_url_after": new_canonical_url,
                }
            )
            if not dry_run:
                docs_repo.upsert_document(
                    Document(
                        document_id=doc.document_id,
                        source=doc.source,
                        source_uri=doc.source_uri,
                        canonical_url=new_canonical_url,
                        title=new_title,
                        author=doc.author,
                        published_year=doc.published_year,
                        language=doc.language,
                        content_type=doc.content_type,
                        is_scanned=doc.is_scanned,
                        status=doc.status,
                        content_fingerprint=doc.content_fingerprint,
                        canonical_sha256=doc.canonical_sha256,
                        canonical_etag=doc.canonical_etag,
                        categories_json=doc.categories_json,
                        source_dataset=doc.source_dataset,
                    )
                )
            updated += 1

    logger.info("Metadata backfill complete: updated=%s dry_run=%s", updated, dry_run)
    return {
        "dry_run": dry_run,
        "max_docs": max_docs,
        "sources_csv": sources_csv,
        "updated": updated,
        "actions": actions,
        "generated_at": datetime.now(UTC).isoformat(),
    }
