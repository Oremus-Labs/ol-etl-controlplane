from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.events import DocsDiscoveredEvent
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.models import Document, DocumentLink
from ol_rag_pipeline_core.nats_publisher import publish_json_sync
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.sources.vatican_sqlite import discover_url_rows
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


@flow(name="vatican_sqlite_sync_flow")
def vatican_sqlite_sync_flow() -> dict[str, int]:
    settings = load_settings()
    logger = get_run_logger()

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    vpn_guard = VpnRotationGuard(
        gluetun=GluetunHttpControlClient(
            GluetunConfig(control_url=settings.gluetun_control_url, api_key=settings.gluetun_api_key)
        ),
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

    db_key = f"datasets/vatican_sqlite/{settings.dataset_version}/vatican.db"
    db_bytes = s3.get_bytes(db_key)

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "vatican.db"
        path.write_bytes(db_bytes)
        rows = discover_url_rows(str(path), limit=settings.vatican_sqlite_max_rows)

    created = 0
    skipped = 0
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for row in rows:
            source = "vatican_sqlite"
            source_uri = row.url
            document_id = stable_document_id(source, source_uri)

            rotated = vpn_guard.before_request(source_uri)
            if rotated:
                logger.info("Rotated VPN after %s external requests", settings.vpn_rotate_every_n_requests)

            r = client.get(source_uri)
            if r.status_code >= 400:
                continue
            body = r.content
            content_type = r.headers.get("content-type")
            fingerprint = sha256_bytes(body)

            filename = urlparse(source_uri).path.rstrip("/").split("/")[-1] or f"{row.row_id}.bin"
            with connect(dsn, schema="public") as conn:
                docs = DocumentRepository(conn)
                files_repo = DocumentFileRepository(conn)

                existing = docs.get_document(document_id)
                if existing and existing.content_fingerprint == fingerprint:
                    skipped += 1
                    continue

                raw_key = f"library/raw/{source}/{document_id}/{filename}"
                raw_uri = s3.put_bytes(raw_key, body, content_type=content_type)

                docs.upsert_document(
                    Document(
                        document_id=document_id,
                        source=source,
                        source_uri=source_uri,
                        content_fingerprint=fingerprint,
                        content_type=content_type,
                        status="discovered",
                        source_dataset=f"sqlite:{settings.dataset_version}",
                    )
                )
                files_repo.upsert_file(
                    document_id=document_id,
                    variant="raw",
                    storage_uri=raw_uri,
                    sha256=fingerprint,
                    bytes_size=len(body),
                    mime_type=content_type,
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
                    hints={"content_type": content_type, "is_scanned": None},
                )
                publish_json_sync(settings.nats_url, settings.nats_subject, event.model_dump_json())
                created += 1

    logger.info("Vatican SQLite sync complete: created=%s skipped=%s", created, skipped)
    return {"created": created, "skipped": skipped}
