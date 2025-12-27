from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import time
from uuid import uuid4

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from ol_etl_controlplane.config import load_settings


@dataclass(frozen=True)
class DocumentWork:
    document_id: str
    status: str
    content_fingerprint: str | None
    source: str | None


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


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _pick_deployment_fqn(document_id: str, fqns: list[str], default_fqn: str) -> str:
    if not fqns:
        return default_fqn
    if len(fqns) == 1:
        return fqns[0]
    digest = hashlib.sha256(document_id.encode("utf-8")).hexdigest()
    return fqns[int(digest, 16) % len(fqns)]


def _in_partition(document_id: str, partition_index: int, num_partitions: int) -> bool:
    if num_partitions <= 1:
        return True
    digest = hashlib.sha256(document_id.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_partitions == partition_index


def _fetch_documents(
    conn,
    statuses: list[str],
    source: str | None,
    min_updated_age_minutes: int | None,
) -> list[DocumentWork]:
    if not statuses:
        return []
    placeholders = ", ".join(["%s"] * len(statuses))
    query = (
        "select document_id, status, content_fingerprint, source "
        "from documents "
        f"where status in ({placeholders})"
    )
    params: list[object] = list(statuses)
    if source:
        query += " and source = %s"
        params.append(source)
    if min_updated_age_minutes and min_updated_age_minutes > 0:
        query += " and updated_at <= now() - (%s || ' minutes')::interval"
        params.append(int(min_updated_age_minutes))
    query += " order by updated_at asc"
    rows = conn.execute(query, params).fetchall()
    return [
        DocumentWork(
            document_id=row[0],
            status=row[1],
            content_fingerprint=row[2],
            source=row[3],
        )
        for row in rows
    ]


def _iter_batches(items: list[DocumentWork], batch_size: int) -> list[list[DocumentWork]]:
    if batch_size <= 0:
        return [items]
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _enqueue_documents(
    *,
    documents: list[DocumentWork],
    deployment_fqns: list[str],
    default_fqn: str,
    pipeline_version: str,
    kind: str,
    force_process: bool,
    dry_run: bool,
    rate_sleep_s: float,
    batch_size: int,
    logger,
) -> int:
    created = 0
    batches = _iter_batches(documents, batch_size)
    for idx, batch in enumerate(batches, start=1):
        for doc in batch:
            deployment = _pick_deployment_fqn(doc.document_id, deployment_fqns, default_fqn)
            params: dict[str, object] = {
                "document_id": doc.document_id,
                "pipeline_version": pipeline_version,
                "source": doc.source,
                "event_id": str(uuid4()),
            }
            if doc.content_fingerprint:
                params["content_fingerprint"] = doc.content_fingerprint
            if kind == "process":
                params["force"] = bool(force_process)

            if dry_run:
                logger.info("Dry run: would enqueue %s via %s", doc.document_id, deployment)
                created += 1
                continue

            fr = run_deployment(
                name=deployment,
                parameters=params,
                timeout=0,
                as_subflow=False,
            )
            flow_run_id = getattr(fr, "id", fr)
            logger.info("Enqueued %s via %s flow_run_id=%s", doc.document_id, deployment, flow_run_id)
            created += 1

        if rate_sleep_s > 0 and idx < len(batches):
            time.sleep(rate_sleep_s)
    return created


@flow(name="process_index_backfill_flow", retries=2, retry_delay_seconds=60)
def process_index_backfill_flow(
    *,
    source: str | None = "vatican_sqlite",
    process_statuses_csv: str = "discovered",
    index_statuses_csv: str = "extracted",
    partition_index: int = 0,
    num_partitions: int = 1,
    min_updated_age_minutes: int | None = 15,
    max_docs: int | None = None,
    batch_size: int = 200,
    rate_sleep_s: float = 0.0,
    dry_run: bool = False,
    force_process: bool = False,
) -> dict[str, object]:
    """
    Backfill processing + indexing for already-downloaded documents.

    Each run handles a hash partition of documents to keep runs small and restartable.
    """
    settings = load_settings()
    logger = get_run_logger()

    dsn = _pg_dsn_from_env(settings)
    apply_migrations(dsn, schema="public")

    process_statuses = _parse_csv(process_statuses_csv)
    index_statuses = _parse_csv(index_statuses_csv)

    process_deployments = _parse_csv(settings.process_deployment_fqns)
    index_deployments = _parse_csv(settings.index_deployment_fqns)

    pipeline_version = settings.pipeline_version

    with connect(dsn, schema="public") as conn:
        process_docs = _fetch_documents(
            conn, process_statuses, source, min_updated_age_minutes
        )
        index_docs = _fetch_documents(conn, index_statuses, source, min_updated_age_minutes)

    process_docs = [
        doc for doc in process_docs if _in_partition(doc.document_id, partition_index, num_partitions)
    ]
    index_docs = [
        doc for doc in index_docs if _in_partition(doc.document_id, partition_index, num_partitions)
    ]

    if max_docs is not None and max_docs > 0:
        process_docs = process_docs[: max_docs]
        index_docs = index_docs[: max_docs]

    logger.info(
        "Backfill partition %s/%s: process=%s index=%s dry_run=%s min_age_minutes=%s",
        partition_index,
        num_partitions,
        len(process_docs),
        len(index_docs),
        dry_run,
        min_updated_age_minutes,
    )

    process_created = _enqueue_documents(
        documents=process_docs,
        deployment_fqns=process_deployments,
        default_fqn="process_document_flow/process-document",
        pipeline_version=pipeline_version,
        kind="process",
        force_process=force_process,
        dry_run=dry_run,
        rate_sleep_s=rate_sleep_s,
        batch_size=batch_size,
        logger=logger,
    )

    index_created = _enqueue_documents(
        documents=index_docs,
        deployment_fqns=index_deployments,
        default_fqn="index_document_flow/index-document",
        pipeline_version=pipeline_version,
        kind="index",
        force_process=False,
        dry_run=dry_run,
        rate_sleep_s=rate_sleep_s,
        batch_size=batch_size,
        logger=logger,
    )

    return {
        "source": source,
        "partition_index": partition_index,
        "num_partitions": num_partitions,
        "min_updated_age_minutes": min_updated_age_minutes,
        "process_enqueued": process_created,
        "index_enqueued": index_created,
        "dry_run": dry_run,
        "generated_at": datetime.now(UTC).isoformat(),
    }
