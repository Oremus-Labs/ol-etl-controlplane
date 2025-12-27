from __future__ import annotations

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from ol_etl_controlplane.config import load_settings


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def _pick_deployment_fqn(partition_index: int, fqns: list[str]) -> str:
    if len(fqns) == 1:
        return fqns[0]
    return fqns[partition_index % len(fqns)]


@flow(name="process_index_backfill_enqueue_flow")
def process_index_backfill_enqueue_flow(
    *,
    num_partitions: int = 10,
    start_partition: int = 0,
    end_partition: int | None = None,
    deployment_fqn: str = "process_index_backfill_flow/process-index-backfill",
    source: str | None = "vatican_sqlite",
    process_statuses_csv: str = "discovered",
    index_statuses_csv: str = "extracted",
    max_docs: int | None = None,
    batch_size: int = 200,
    rate_sleep_s: float = 0.0,
    dry_run: bool = False,
    force_process: bool = False,
) -> dict[str, object]:
    """
    Enqueue partitioned process/index backfill runs.

    This spreads scheduling load across multiple work queues and keeps runs small.
    """
    settings = load_settings()
    logger = get_run_logger()

    num_partitions = int(num_partitions)
    if num_partitions <= 0:
        raise ValueError("num_partitions must be > 0")

    start_partition = int(start_partition)
    if start_partition < 0 or start_partition >= num_partitions:
        raise ValueError("start_partition must be within [0, num_partitions)")

    if end_partition is None:
        end_partition = num_partitions - 1
    end_partition = int(end_partition)
    if end_partition < start_partition or end_partition >= num_partitions:
        raise ValueError("end_partition must be within [start_partition, num_partitions)")

    deployment_fqns = (
        _parse_csv(settings.process_index_backfill_deployment_fqns) or [deployment_fqn]
    )

    created = 0
    for partition_index in range(start_partition, end_partition + 1):
        params: dict[str, object] = {
            "partition_index": partition_index,
            "num_partitions": num_partitions,
            "source": source,
            "process_statuses_csv": process_statuses_csv,
            "index_statuses_csv": index_statuses_csv,
            "batch_size": batch_size,
            "rate_sleep_s": rate_sleep_s,
            "dry_run": dry_run,
            "force_process": force_process,
        }
        if max_docs is not None:
            params["max_docs"] = int(max_docs)

        selected_fqn = _pick_deployment_fqn(partition_index, deployment_fqns)
        fr = run_deployment(name=selected_fqn, parameters=params)
        flow_run_id = getattr(fr, "id", fr)
        logger.info(
            "Enqueued process/index backfill: partition=%s/%s flow_run_id=%s",
            partition_index,
            num_partitions,
            flow_run_id,
        )
        created += 1

    return {
        "enqueued": created,
        "num_partitions": num_partitions,
        "deployment_fqns": deployment_fqns,
    }
