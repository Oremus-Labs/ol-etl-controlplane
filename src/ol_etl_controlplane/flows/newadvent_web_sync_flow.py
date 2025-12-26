from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import uuid4

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.events import DocsDiscoveredEvent
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.models import Document, DocumentLink
from ol_rag_pipeline_core.nats_publisher import publish_json_sync
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.sources.newadvent_web import fetch_pages
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from ol_rag_pipeline_core.util import sha256_bytes, stable_document_id
from ol_rag_pipeline_core.vpn import GluetunConfig, GluetunHttpControlClient, VpnRotationGuard
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


def _default_seed_urls() -> list[str]:
    return [
        "https://www.newadvent.org/fathers/0101.htm",
        "https://www.newadvent.org/fathers/0102.htm",
    ]


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


@flow(name="newadvent_web_sync_flow")
def newadvent_web_sync_flow() -> dict[str, int]:
    settings = load_settings()
    logger = get_run_logger()

    if not settings.nats_url:
        raise RuntimeError("Missing NATS_URL")
    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    proxy_pool = _parse_csv(settings.vpn_http_proxy_pool)
    vpn_guard = VpnRotationGuard(
        gluetun=(
            None
            if proxy_pool
            else GluetunHttpControlClient(
                GluetunConfig(
                    control_url=settings.gluetun_control_url, api_key=settings.gluetun_api_key
                )
            )
        ),
        proxy_pool=proxy_pool,
        rotate_every_n_requests=settings.vpn_rotate_every_n_requests,
        require_vpn_for_external=settings.vpn_required,
        ensure_timeout_s=settings.vpn_ensure_timeout_s,
        rotate_cooldown_s=settings.vpn_rotate_cooldown_s,
    )

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

    urls = (
        [u.strip() for u in (settings.newadvent_seed_urls or "").split(",") if u.strip()]
        or _default_seed_urls()
    )[: settings.newadvent_web_max_pages]

    pages = fetch_pages(urls, vpn_guard=vpn_guard)
    created = 0
    skipped = 0

    for page in pages:
        source = "newadvent_web"
        source_uri = page.url
        document_id = stable_document_id(source, source_uri)
        fingerprint = sha256_bytes(page.body)

        filename = urlparse(page.url).path.rstrip("/").split("/")[-1] or "index.html"
        if not filename.endswith((".html", ".htm")):
            filename = f"{filename}.html"

        with connect(dsn, schema="public") as conn:
            docs = DocumentRepository(conn)
            files_repo = DocumentFileRepository(conn)

            existing = docs.get_document(document_id)
            if existing and existing.content_fingerprint == fingerprint:
                skipped += 1
                continue

            raw_key = f"library/raw/{source}/{document_id}/{filename}"
            raw_uri = s3.put_bytes(raw_key, page.body, content_type=page.content_type)

            docs.upsert_document(
                Document(
                    document_id=document_id,
                    source=source,
                    source_uri=source_uri,
                    content_fingerprint=fingerprint,
                    content_type=page.content_type,
                    status="discovered",
                    source_dataset="web:live",
                )
            )
            files_repo.upsert_file(
                document_id=document_id,
                variant="raw",
                storage_uri=raw_uri,
                sha256=fingerprint,
                bytes_size=len(page.body),
                mime_type=page.content_type,
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
                hints={"content_type": page.content_type, "is_scanned": None},
            )
            publish_json_sync(settings.nats_url, settings.nats_subject, event.model_dump_json())
            created += 1

    logger.info("New Advent web sync complete: created=%s skipped=%s", created, skipped)
    return {"created": created, "skipped": skipped}
