from __future__ import annotations

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment


@flow(name="vatican_sqlite_enqueue_flow")
def vatican_sqlite_enqueue_flow(
    *,
    num_partitions: int = 128,
    start_partition: int = 0,
    end_partition: int | None = None,
    deployment_fqn: str = "vatican_sqlite_sync_flow/vatican-sqlite-sync",
    max_rows: int | None = 0,
) -> dict[str, int]:
    """
    Enqueue partitioned Vatican sqlite sync runs.

    This creates many small, independent flow runs so 8 workers can process them in parallel
    and retries are easy per-partition.
    """
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

    created = 0
    for partition_index in range(start_partition, end_partition + 1):
        params: dict[str, object] = {
            "partition_index": partition_index,
            "num_partitions": num_partitions,
        }
        if max_rows is not None:
            params["max_rows"] = int(max_rows)

        fr = run_deployment(name=deployment_fqn, parameters=params)
        flow_run_id = getattr(fr, "id", fr)
        logger.info(
            "Enqueued vatican sqlite sync: partition=%s/%s flow_run_id=%s",
            partition_index,
            num_partitions,
            flow_run_id,
        )
        created += 1

    return {"enqueued": created, "num_partitions": num_partitions}
