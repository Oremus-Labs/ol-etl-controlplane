from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.sources.vatican_sqlite import discover_document_rows
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from ol_rag_pipeline_core.util import stable_document_id
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

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


def _is_excluded_url(url: str, *, exclude_urls: set[str], exclude_prefixes: list[str]) -> bool:
    if url in exclude_urls:
        return True
    return any(url.startswith(prefix) for prefix in exclude_prefixes)


@flow(name="vatican_sqlite_reconcile_missing_flow")
def vatican_sqlite_reconcile_missing_flow(
    *,
    batch_size: int = 25,
    max_urls: int = 500,
    include_missing_raw: bool = True,
    publish_events: bool = True,
    dry_run: bool = True,
    refetch_deployment_fqn: str = "vatican_sqlite_refetch_batch_flow/vatican-sqlite-refetch-batch",
) -> dict[str, Any]:
    """
    Operator tool: find Vatican sqlite rows that are missing in Postgres (or missing raw bytes)
    and enqueue targeted refetch batches.

    This is the "nothing gets missed" button:
    - It does NOT re-fetch the whole corpus.
    - It only schedules refetches for URLs where we have a gap.
    """
    settings = load_settings()
    logger = get_run_logger()

    batch_size = max(1, int(batch_size))
    max_urls = max(0, int(max_urls))
    dry_run = bool(dry_run)
    refetch_deployment_fqns = _parse_csv(settings.vatican_sqlite_refetch_deployment_fqns) or [
        refetch_deployment_fqn
    ]

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

    db_key = f"datasets/vatican_sqlite/{settings.dataset_version}/vatican.db"
    db_bytes = s3.get_bytes(db_key)
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "vatican.db"
        path.write_bytes(db_bytes)
        hosts = _parse_csv(settings.vatican_sqlite_hosts)
        rows = discover_document_rows(
            str(path),
            limit=0,
            hosts=hosts,
            sample_per_host=settings.vatican_sqlite_sample_per_host,
        )

    exclude_urls = {
        u.strip()
        for u in (settings.vatican_sqlite_exclude_urls or "").split(",")
        if u.strip()
    }
    exclude_prefixes = _parse_prefixes(settings.vatican_sqlite_exclude_prefixes)

    urls_from_sqlite = []
    for row in rows:
        if not row.url:
            continue
        if _is_excluded_url(
            row.url, exclude_urls=exclude_urls, exclude_prefixes=exclude_prefixes
        ):
            continue
        urls_from_sqlite.append(row.url)

    with connect(dsn, schema="public") as conn:
        existing_ids = {
            r[0]
            for r in conn.execute(
                "select document_id from documents where source='vatican_sqlite'"
            ).fetchall()
        }

        missing_urls: list[str] = []
        for url in urls_from_sqlite:
            doc_id = stable_document_id("vatican_sqlite", url)
            if doc_id not in existing_ids:
                missing_urls.append(url)

        missing_raw_urls: list[str] = []
        if include_missing_raw:
            raw_missing_rows = conn.execute(
                """
                select d.source_uri
                from documents d
                left join document_files f
                  on f.document_id=d.document_id and f.variant='raw'
                where d.source='vatican_sqlite'
                  and f.document_id is null
                  and d.source_uri is not null
                """
            ).fetchall()
            missing_raw_urls = [r[0] for r in raw_missing_rows if r[0]]
            if exclude_urls or exclude_prefixes:
                missing_raw_urls = [
                    url
                    for url in missing_raw_urls
                    if not _is_excluded_url(
                        url, exclude_urls=exclude_urls, exclude_prefixes=exclude_prefixes
                    )
                ]

    # De-dupe while preserving order (missing first).
    seen: set[str] = set()
    candidate_urls: list[str] = []
    for url in missing_urls + missing_raw_urls:
        if url in seen:
            continue
        seen.add(url)
        candidate_urls.append(url)

    if max_urls and len(candidate_urls) > max_urls:
        candidate_urls = candidate_urls[:max_urls]

    logger.info(
        "Vatican reconcile missing: sqlite_urls=%s missing_docs=%s missing_raw=%s enqueue=%s batch_size=%s",
        len(urls_from_sqlite),
        len(missing_urls),
        len(missing_raw_urls),
        len(candidate_urls),
        batch_size,
    )

    if dry_run:
        return {
            "dry_run": True,
            "sqlite_urls": len(urls_from_sqlite),
            "missing_docs": len(missing_urls),
            "missing_raw": len(missing_raw_urls),
            "would_enqueue_urls": len(candidate_urls),
            "batch_size": batch_size,
            "max_urls": max_urls,
            "publish_events": publish_events,
            "refetch_deployment_fqns": refetch_deployment_fqns,
            "candidate_urls_sample": candidate_urls[: min(20, len(candidate_urls))],
        }

    enqueued = 0
    flow_run_ids: list[str] = []
    for idx, i in enumerate(range(0, len(candidate_urls), batch_size)):
        batch = candidate_urls[i : i + batch_size]
        selected_fqn = refetch_deployment_fqns[idx % len(refetch_deployment_fqns)]
        fr = run_deployment(
            name=selected_fqn,
            parameters={"urls": batch, "publish_events": publish_events},
        )
        flow_run_id = getattr(fr, "id", fr)
        flow_run_ids.append(str(flow_run_id))
        enqueued += 1

    return {
        "dry_run": False,
        "sqlite_urls": len(urls_from_sqlite),
        "missing_docs": len(missing_urls),
        "missing_raw": len(missing_raw_urls),
        "enqueued_batches": enqueued,
        "batch_size": batch_size,
        "max_urls": max_urls,
        "publish_events": publish_events,
        "refetch_deployment_fqns": refetch_deployment_fqns,
        "flow_run_ids": flow_run_ids,
    }
