from __future__ import annotations

import tempfile
import time
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
from ol_rag_pipeline_core.sources.archive_org import is_archive_details_url, resolve_and_download_pdf
from ol_rag_pipeline_core.sources.vatican_sqlite import discover_document_rows
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


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


@flow(name="vatican_sqlite_sync_flow")
def vatican_sqlite_sync_flow(
    partition_index: int | None = None,
    num_partitions: int | None = None,
    allow_single_run: bool = False,
    max_rows: int | None = None,
) -> dict[str, int]:
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
    if settings.vpn_required:
        vpn_guard.ensure_vpn_running()

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

    effective_max_rows = settings.vatican_sqlite_max_rows if max_rows is None else int(max_rows)
    if (partition_index is None) != (num_partitions is None):
        raise ValueError("partition_index and num_partitions must be set together (or both unset)")
    if num_partitions is None:
        # Safety rail: prevent accidental single-run bulk ingest (should be partitioned).
        if effective_max_rows > 2000 and not allow_single_run:
            raise RuntimeError(
                "Refusing to run Vatican sync as a single long job. "
                "Use vatican_sqlite_enqueue_flow to enqueue partitioned runs, "
                "or re-run with allow_single_run=true for a one-off."
            )
    else:
        num_partitions = int(num_partitions)
        partition_index = int(partition_index or 0)
        if num_partitions <= 0:
            raise ValueError("num_partitions must be > 0")
        if partition_index < 0 or partition_index >= num_partitions:
            raise ValueError("partition_index must be within [0, num_partitions)")

    hosts = _parse_csv(settings.vatican_sqlite_hosts)
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "vatican.db"
        path.write_bytes(db_bytes)
        rows = discover_document_rows(
            str(path),
            limit=effective_max_rows,
            hosts=hosts,
            sample_per_host=settings.vatican_sqlite_sample_per_host,
            partition_index=partition_index,
            num_partitions=num_partitions,
        )
    if hosts:
        logger.info(
            "Vatican SQLite host filter enabled: hosts=%s sample_per_host=%s rows=%s",
            ",".join(hosts),
            settings.vatican_sqlite_sample_per_host,
            len(rows),
        )
    exclude_urls = {
        u.strip()
        for u in (settings.vatican_sqlite_exclude_urls or "").split(",")
        if u.strip()
    }
    if exclude_urls:
        before = len(rows)
        rows = [r for r in rows if r.url not in exclude_urls]
        logger.info(
            "Vatican SQLite exclude URLs enabled: excluded=%s removed=%s remaining=%s",
            len(exclude_urls),
            before - len(rows),
            len(rows),
        )
    if num_partitions is not None:
        logger.info(
            "Vatican SQLite partition enabled: partition=%s/%s rows=%s",
            partition_index,
            num_partitions,
            len(rows),
        )

    created = 0
    skipped = 0
    failed = 0
    # Keep attempts snappy: Vatican endpoints can be slow/unreliable (especially over VPN),
    # but we don't want a single URL to stall the whole partition.
    def _timeout_for_attempt(attempt: int) -> httpx.Timeout:
        read_s = min(30.0 * max(1, attempt), 120.0)
        return httpx.Timeout(connect=10.0, read=read_s, write=30.0, pool=10.0)

    default_timeout = _timeout_for_attempt(2)
    headers = {
        "User-Agent": "ol-etl-pipeline/1.0 (+https://oremuslabs.app)",
        "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.7,fr;q=0.7",
        "Connection": "keep-alive",
    }

    def _new_client() -> httpx.Client:
        # Important: VPN rotations restart OpenVPN under the hood; resetting the client clears
        # any stale proxy connections in the pool.
        return httpx.Client(
            timeout=default_timeout,
            follow_redirects=True,
            headers=headers,
            http2=False,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=0),
        )

    client = _new_client()
    try:
        # Reuse a single Postgres connection for the whole flow run.
        # Opening a new connection per row quickly exhausts Postgres max_connections
        # once multiple partitions are running concurrently.
        with connect(dsn, schema="public") as conn:
            conn.autocommit = True
            docs = DocumentRepository(conn)
            files_repo = DocumentFileRepository(conn)

            for row in rows:
                source = "vatican_sqlite"
                source_uri = row.url
                document_id = stable_document_id(source, source_uri)

                rotated = vpn_guard.before_request(source_uri)
                if rotated:
                    logger.info(
                        "Rotated VPN after %s external requests",
                        settings.vpn_rotate_every_n_requests,
                    )
                    client.close()
                    client = _new_client()

                r: httpx.Response | None = None
                for attempt in range(1, 4):
                    try:
                        r = client.get(source_uri, timeout=_timeout_for_attempt(attempt))
                    except httpx.RequestError as e:
                        logger.warning(
                            "Fetch failed (attempt %s/3) url=%s err=%s",
                            attempt,
                            source_uri,
                            repr(e),
                        )
                        if settings.vpn_required:
                            vpn_guard.rotate_vpn()
                            client.close()
                            client = _new_client()
                        # Avoid hammering endpoints/proxy after a disconnect/timeout.
                        time.sleep(min(10.0, 1.5 * attempt))
                        continue

                    if r.status_code in {403, 429, 500, 502, 503, 504}:
                        logger.warning(
                            "Fetch got %s (attempt %s/3) url=%s; rotating VPN",
                            r.status_code,
                            attempt,
                            source_uri,
                        )
                        if settings.vpn_required:
                            vpn_guard.rotate_vpn()
                            client.close()
                            client = _new_client()
                        time.sleep(min(10.0, 1.5 * attempt))
                        continue
                    break

                if not r or r.status_code >= 400:
                    failed += 1
                    continue
                body = r.content
                content_type = r.headers.get("content-type")

                # archive.org /details pages are landing HTML; prefer downloading an actual PDF from /download/.
                if is_archive_details_url(source_uri):
                    try:
                        resolved = resolve_and_download_pdf(client=client, details_url=source_uri)
                    except Exception as e:  # noqa: BLE001
                        logger.warning(
                            "archive.org resolve failed: url=%s err=%s", source_uri, repr(e)
                        )
                        resolved = None
                    if resolved and resolved.body:
                        body = resolved.body
                        content_type = resolved.content_type or "application/pdf"
                fingerprint = sha256_bytes(body)

                filename = (
                    urlparse(source_uri).path.rstrip("/").split("/")[-1]
                    or f"{row.row_id}.bin"
                )

                existing = docs.get_document(document_id)
                if existing:
                    status = existing.status
                    is_scanned = existing.is_scanned
                else:
                    status = "discovered"
                    is_scanned = None

                # Always upsert metadata even if the raw content fingerprint did not change.
                # This lets us backfill title/year/author/categories for already-ingested docs.
                doc_categories = [c for c in (row.categories or []) if c]
                docs.upsert_document(
                    Document(
                        document_id=document_id,
                        source=source,
                        source_uri=source_uri,
                        canonical_url=source_uri,
                        title=row.title,
                        author=row.author,
                        published_year=row.year,
                        language=row.language,
                        content_fingerprint=fingerprint,
                        content_type=content_type,
                        is_scanned=is_scanned,
                        status=status,
                        categories_json={
                            "categories": doc_categories,
                            "publisher": row.publisher,
                            "short_title": row.short_title,
                            "display_year": row.display_year,
                            "raw": row.raw_json,
                            "source_row_id": row.row_id,
                        },
                        source_dataset=f"sqlite:{settings.dataset_version}",
                    )
                )
                for c in doc_categories:
                    docs.add_category(document_id, c)
                docs.add_link(
                    DocumentLink(
                        document_id=document_id,
                        link_type="source_row_id",
                        url=f"vatican-sqlite://documents/{row.row_id}",
                    )
                )
                if row.bibliography:
                    docs.add_link(
                        DocumentLink(
                            document_id=document_id,
                            link_type="bibliography",
                            url=row.bibliography,
                        )
                    )

                if existing and existing.content_fingerprint == fingerprint:
                    skipped += 1
                    continue

                raw_key = f"library/raw/{source}/{document_id}/{filename}"
                raw_uri = s3.put_bytes(raw_key, body, content_type=content_type)

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
                    hints={
                        "content_type": content_type,
                        "is_scanned": None,
                        "title": row.title,
                        "author": row.author,
                        "published_year": row.year,
                        "language": row.language,
                        "categories": row.categories,
                        "source_row_id": row.row_id,
                    },
                )
                publish_json_sync(
                    settings.nats_url, settings.nats_subject, event.model_dump_json()
                )
                created += 1
                if (created + skipped + failed) % 250 == 0:
                    logger.info(
                        "Vatican sync progress: processed=%s created=%s skipped=%s failed=%s",
                        created + skipped + failed,
                        created,
                        skipped,
                        failed,
                    )
    finally:
        client.close()

    logger.info(
        "Vatican SQLite sync complete: created=%s skipped=%s failed=%s",
        created,
        skipped,
        failed,
    )
    return {"created": created, "skipped": skipped, "failed": failed}
