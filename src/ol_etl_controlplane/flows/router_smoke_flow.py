from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger


@flow(name="router_smoke_flow")
def router_smoke_flow(
    event: dict[str, Any],
    event_id: str,
    document_id: str,
    pipeline_version: str,
    content_fingerprint: str,
) -> None:
    logger = get_run_logger()
    logger.info(
        "router_smoke_flow received event",
        extra={
            "event_id": event_id,
            "document_id": document_id,
            "pipeline_version": pipeline_version,
            "content_fingerprint": content_fingerprint,
        },
    )
    logger.info("event keys: %s", sorted(event.keys()))

