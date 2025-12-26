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


def _parse_prefixes(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _host_matches_suffix(host: str, suffixes: list[str]) -> bool:
    for suffix in suffixes:
        if host == suffix:
            return True
        if suffix and host.endswith(f".{suffix}"):
            return True
    return False


def _is_excluded_url(url: str, *, exclude_urls: set[str], exclude_prefixes: list[str]) -> bool:
    if url in exclude_urls:
        return True
    return any(url.startswith(prefix) for prefix in exclude_prefixes)


def _headers_for_url(url: str, base_headers: dict[str, str]) -> dict[str, str]:
    headers = dict(base_headers)
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
        headers["Referer"] = f"{origin}/"
        headers["Origin"] = origin
    return headers


def _can_bypass_vpn_on_403(url: str, settings) -> bool:  # noqa: ANN001
    if not settings.vatican_http_direct_on_403:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    exclude_suffixes = _parse_csv(settings.vatican_http_direct_exclude_domains) or []
    return not _host_matches_suffix(host, exclude_suffixes)


def _timeout_for_attempt(attempt: int) -> httpx.Timeout:
    read_s = min(30.0 * max(1, attempt), 120.0)
    return httpx.Timeout(connect=10.0, read=read_s, write=30.0, pool=10.0)


@flow(name="vatican_sqlite_refetch_batch_flow")
def vatican_sqlite_refetch_batch_flow(
    *,
    urls: list[str],
    publish_events: bool = True,
    force: bool = False,
) -> dict[str, int]:
    """
    Targeted refetch for Vatican sqlite URLs.

    Why this exists:
    - `vatican_sqlite_sync_flow` only writes a document after a successful fetch.
      If a URL times out/disconnects, it can be "missed" (no Postgres row).
    - Re-running the full sync would re-fetch everything; this flow fetches ONLY a provided list.

    Behavior:
    - Skips URLs whose `document_id` already has a `document_files.variant=raw` unless `force=true`
    - Upserts metadata from the sqlite row (title/author/year/categories) when available
    - Stores raw bytes to MinIO and (optionally) publishes `docs.discovered` events for downstream processing
    """
    settings = load_settings()
    logger = get_run_logger()

    if not urls:
        return {"requested": 0, "fetched": 0, "skipped": 0, "failed": 0, "published": 0}

    if not settings.nats_url:
        raise RuntimeError("Missing NATS_URL")
    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    proxy_pool = _parse_csv(settings.vpn_http_proxy_pool)
    vpn_guard = VpnRotationGuard(
        gluetun=GluetunHttpControlClient(
            GluetunConfig(control_url=settings.gluetun_control_url, api_key=settings.gluetun_api_key)
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
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "vatican.db"
        path.write_bytes(db_bytes)
        rows = discover_document_rows(str(path), limit=0)

    # Build a URL->row mapping for metadata enrichment.
    row_by_url = {r.url: r for r in rows if r.url}

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.7,fr;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    def _new_client() -> httpx.Client:
        return httpx.Client(
            timeout=_timeout_for_attempt(2),
            follow_redirects=True,
            headers=headers,
            http2=False,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=0),
        )

    exclude_urls = {
        u.strip()
        for u in (settings.vatican_sqlite_exclude_urls or "").split(",")
        if u.strip()
    }
    exclude_prefixes = _parse_prefixes(settings.vatican_sqlite_exclude_prefixes)

    requested = 0
    fetched = 0
    skipped = 0
    failed = 0
    published = 0

    client = _new_client()
    direct_client = httpx.Client(
        timeout=_timeout_for_attempt(2),
        follow_redirects=True,
        headers=headers,
        http2=False,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=0),
        trust_env=False,
    )
    try:
        with connect(dsn, schema="public") as conn:
            conn.autocommit = True
            docs = DocumentRepository(conn)
            files_repo = DocumentFileRepository(conn)

            for source_uri in urls:
                source_uri = (source_uri or "").strip()
                if not source_uri:
                    continue
                if _is_excluded_url(
                    source_uri, exclude_urls=exclude_urls, exclude_prefixes=exclude_prefixes
                ):
                    skipped += 1
                    continue
                requested += 1

                source = "vatican_sqlite"
                document_id = stable_document_id(source, source_uri)

                existing = docs.get_document(document_id)
                existing_raw = files_repo.get_file(document_id=document_id, variant="raw")
                if existing_raw and not force:
                    skipped += 1
                    continue

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
                        r = client.get(
                            source_uri,
                            timeout=_timeout_for_attempt(attempt),
                            headers=_headers_for_url(source_uri, headers),
                        )
                    except httpx.RequestError as e:
                        logger.warning(
                            "Refetch failed (attempt %s/3) url=%s err=%s",
                            attempt,
                            source_uri,
                            repr(e),
                        )
                        if settings.vpn_required:
                            vpn_guard.rotate_vpn()
                            client.close()
                            client = _new_client()
                        time.sleep(min(10.0, 1.5 * attempt))
                        continue

                    if r.status_code == 403 and _can_bypass_vpn_on_403(source_uri, settings):
                        logger.warning(
                            "Refetch got 403 url=%s; trying direct (no VPN/proxy)",
                            source_uri,
                        )
                        try:
                            direct = direct_client.get(
                                source_uri,
                                timeout=_timeout_for_attempt(attempt),
                                headers=_headers_for_url(source_uri, headers),
                            )
                        except httpx.RequestError as e:
                            logger.warning(
                                "Direct refetch failed (attempt %s/3) url=%s err=%s",
                                attempt,
                                source_uri,
                                repr(e),
                            )
                            direct = None
                        if direct and direct.status_code < 400:
                            r = direct
                            break
                        if direct is not None:
                            logger.warning(
                                "Direct refetch got %s (attempt %s/3) url=%s",
                                direct.status_code,
                                attempt,
                                source_uri,
                            )

                    if r.status_code in {403, 429, 500, 502, 503, 504}:
                        logger.warning(
                            "Refetch got %s (attempt %s/3) url=%s; rotating VPN",
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

                if is_archive_details_url(source_uri):
                    try:
                        resolved = resolve_and_download_pdf(client=client, details_url=source_uri)
                    except Exception as e:  # noqa: BLE001
                        logger.warning("archive.org resolve failed: url=%s err=%s", source_uri, repr(e))
                        resolved = None
                    if resolved and resolved.body:
                        body = resolved.body
                        content_type = resolved.content_type or "application/pdf"

                fingerprint = sha256_bytes(body)
                row = row_by_url.get(source_uri)

                status = existing.status if existing else "discovered"
                is_scanned = existing.is_scanned if existing else None

                doc_categories = [c for c in (row.categories or []) if c] if row else []
                docs.upsert_document(
                    Document(
                        document_id=document_id,
                        source=source,
                        source_uri=source_uri,
                        canonical_url=source_uri,
                        title=(row.title if row else None),
                        author=(row.author if row else None),
                        published_year=(row.year if row else None),
                        language=(row.language if row else None),
                        content_fingerprint=fingerprint,
                        content_type=content_type,
                        is_scanned=is_scanned,
                        status=status,
                        categories_json={
                            "categories": doc_categories,
                            "publisher": (row.publisher if row else None),
                            "short_title": (row.short_title if row else None),
                            "display_year": (row.display_year if row else None),
                            "raw": (row.raw_json if row else None),
                            "source_row_id": (row.row_id if row else None),
                        },
                        source_dataset=f"sqlite:{settings.dataset_version}",
                    )
                )
                for c in doc_categories:
                    docs.add_category(document_id, c)
                if row and row.row_id:
                    docs.add_link(
                        DocumentLink(
                            document_id=document_id,
                            link_type="source_row_id",
                            url=f"vatican-sqlite://documents/{row.row_id}",
                        )
                    )
                if row and row.bibliography:
                    docs.add_link(
                        DocumentLink(
                            document_id=document_id,
                            link_type="bibliography",
                            url=row.bibliography,
                        )
                    )

                filename = urlparse(source_uri).path.rstrip("/").split("/")[-1] or (
                    f"{row.row_id}.bin" if row and row.row_id else "document.bin"
                )
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
                docs.add_link(DocumentLink(document_id=document_id, link_type="source_uri", url=source_uri))
                docs.add_link(DocumentLink(document_id=document_id, link_type="raw_object", url=raw_uri))

                fetched += 1

                should_publish = publish_events and (
                    existing is None
                    or existing.content_fingerprint != fingerprint
                    or existing.status not in {"indexed_ok", "needs_review", "needs_ocr"}
                )
                if should_publish:
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
                            "title": (row.title if row else None),
                            "author": (row.author if row else None),
                            "published_year": (row.year if row else None),
                            "language": (row.language if row else None),
                            "categories": (row.categories if row else None),
                            "source_row_id": (row.row_id if row else None),
                        },
                    )
                    publish_json_sync(
                        settings.nats_url, settings.nats_subject, event.model_dump_json()
                    )
                    published += 1
    finally:
        client.close()
        direct_client.close()

    logger.info(
        "Vatican sqlite refetch complete: requested=%s fetched=%s skipped=%s failed=%s published=%s",
        requested,
        fetched,
        skipped,
        failed,
        published,
    )
    return {
        "requested": requested,
        "fetched": fetched,
        "skipped": skipped,
        "failed": failed,
        "published": published,
    }
