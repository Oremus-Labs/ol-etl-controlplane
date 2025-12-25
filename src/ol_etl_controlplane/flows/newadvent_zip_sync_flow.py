from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.events import DocsDiscoveredEvent
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.models import Document, DocumentLink
from ol_rag_pipeline_core.nats_publisher import publish_json_sync
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.html_head import extract_html_head_metadata
from ol_rag_pipeline_core.sources.newadvent_zip import iter_zip_entries
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from ol_rag_pipeline_core.util import sha256_bytes, stable_document_id
from prefect import flow, get_run_logger

from ol_etl_controlplane.config import load_settings


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


_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _slug(s: str, *, max_len: int = 60) -> str:
    s = (s or "").strip().lower()
    s = _NON_ALNUM.sub("-", s).strip("-")
    if not s:
        return ""
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    return s


def _canonical_url_for_zip_path(path: str) -> str:
    return "https://www.newadvent.org/" + path.lstrip("/")


@flow(name="newadvent_zip_sync_flow")
def newadvent_zip_sync_flow() -> dict[str, int]:
    settings = load_settings()
    logger = get_run_logger()

    if not settings.nats_url:
        raise RuntimeError("Missing NATS_URL")
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

    zip_key = f"datasets/newadvent_zip/{settings.dataset_version}/newadvent.zip"
    zip_bytes = s3.get_bytes(zip_key)
    include_prefixes = (
        [p.strip() for p in (settings.newadvent_zip_include_prefixes or "").split(",") if p.strip()]
        or None
    )
    entries = iter_zip_entries(
        zip_bytes,
        limit=settings.newadvent_zip_max_entries,
        include_prefixes=include_prefixes,
    )
    if include_prefixes:
        logger.info(
            "New Advent ZIP include prefixes enabled: prefixes=%s entries=%s",
            ",".join(include_prefixes),
            len(entries),
        )

    created = 0
    skipped = 0
    for entry in entries:
        source = "newadvent_zip"
        source_uri = f"newadvent_zip://{entry.path}"
        document_id = stable_document_id(source, source_uri)
        fingerprint = sha256_bytes(entry.body)
        canonical_url = _canonical_url_for_zip_path(entry.path)

        collection = (entry.path or "").replace("\\", "/").lstrip("/").split("/", 1)[0].strip().lower()
        collection = collection or "unknown"
        collection_slug = _slug(collection)
        categories = [f"newadvent:collection:{collection_slug}"] if collection_slug else []

        meta = None
        title = None
        og_image = None
        if entry.content_type == "text/html":
            meta = extract_html_head_metadata(entry.body)
            title = meta.title
            og_image = (meta.open_graph.get("og:image") if meta else None) or None

        filename = Path(entry.path).name or "entry.bin"
        with connect(dsn, schema="public") as conn:
            docs = DocumentRepository(conn)
            files_repo = DocumentFileRepository(conn)

            existing = docs.get_document(document_id)
            status = existing.status if existing else "discovered"
            is_scanned = existing.is_scanned if existing else None

            raw_key = f"library/raw/{source}/{document_id}/{filename}"

            docs.upsert_document(
                Document(
                    document_id=document_id,
                    source=source,
                    source_uri=source_uri,
                    canonical_url=canonical_url,
                    title=title,
                    content_fingerprint=fingerprint,
                    content_type=entry.content_type,
                    is_scanned=is_scanned,
                    status=status,
                    categories_json={
                        "zip_path": entry.path,
                        "collection": collection,
                        "meta": (
                            {
                                "canonical_url": meta.canonical_url if meta else None,
                                "description": meta.description if meta else None,
                                "open_graph": meta.open_graph if meta else {},
                                "meta_by_name": (meta.meta_by_name if meta else {}),
                            }
                            if meta
                            else None
                        ),
                    },
                    source_dataset=f"zip:{settings.dataset_version}",
                )
            )
            for c in categories:
                docs.add_category(document_id, c)

            if existing and existing.content_fingerprint == fingerprint:
                skipped += 1
                continue

            raw_uri = s3.put_bytes(raw_key, entry.body, content_type=entry.content_type)
            files_repo.upsert_file(
                document_id=document_id,
                variant="raw",
                storage_uri=raw_uri,
                sha256=fingerprint,
                bytes_size=len(entry.body),
                mime_type=entry.content_type,
            )
            docs.add_link(
                DocumentLink(
                    document_id=document_id,
                    link_type="source_uri",
                    url=source_uri,
                )
            )
            docs.add_link(
                DocumentLink(
                    document_id=document_id,
                    link_type="canonical_url",
                    url=canonical_url,
                )
            )
            if meta and meta.canonical_url and meta.canonical_url != canonical_url:
                docs.add_link(
                    DocumentLink(
                        document_id=document_id,
                        link_type="canonical_url_discovered",
                        url=meta.canonical_url,
                    )
                )
            if og_image:
                docs.add_link(
                    DocumentLink(
                        document_id=document_id,
                        link_type="og_image",
                        url=og_image,
                    )
                )
            docs.add_link(
                DocumentLink(
                    document_id=document_id,
                    link_type="raw_object",
                    url=raw_uri,
                )
            )

            event = DocsDiscoveredEvent(
                event_id=uuid4(),
                document_id=document_id,
                source=source,
                source_uri=source_uri,
                content_fingerprint=fingerprint,
                pipeline_version=settings.pipeline_version,
                discovered_at=datetime.now(UTC),
                hints={
                    "content_type": entry.content_type,
                    "is_scanned": None,
                    "canonical_url": canonical_url,
                    "title": title,
                    "collection": collection,
                    "categories": categories,
                },
            )
            publish_json_sync(settings.nats_url, settings.nats_subject, event.model_dump_json())
            created += 1

    logger.info("New Advent ZIP sync complete: created=%s skipped=%s", created, skipped)
    return {"created": created, "skipped": skipped}
