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


@flow(name="vatican_sqlite_reconcile_missing_flow")
def vatican_sqlite_reconcile_missing_flow(
    *,
    batch_size: int = 25,
    max_urls: int = 500,
    include_missing_raw: bool = True,
    publish_events: bool = True,
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
        rows = discover_document_rows(str(path), limit=0)

    urls_from_sqlite = []
    for row in rows:
        if not row.url:
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

    enqueued = 0
    flow_run_ids: list[str] = []
    for i in range(0, len(candidate_urls), batch_size):
        batch = candidate_urls[i : i + batch_size]
        fr = run_deployment(
            name=refetch_deployment_fqn,
            parameters={"urls": batch, "publish_events": publish_events},
        )
        flow_run_id = getattr(fr, "id", fr)
        flow_run_ids.append(str(flow_run_id))
        enqueued += 1

    return {
        "sqlite_urls": len(urls_from_sqlite),
        "missing_docs": len(missing_urls),
        "missing_raw": len(missing_raw_urls),
        "enqueued_batches": enqueued,
        "batch_size": batch_size,
        "max_urls": max_urls,
        "publish_events": publish_events,
        "refetch_deployment_fqn": refetch_deployment_fqn,
        "flow_run_ids": flow_run_ids,
    }

