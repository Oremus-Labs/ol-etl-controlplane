from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.embedding import EmbeddingClient
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.qdrant import QdrantClient
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
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


@dataclass(frozen=True)
class EvalQuery:
    query: str
    expected_document_ids: list[str] | None = None


def _load_queries(raw_json: str | None) -> list[EvalQuery]:
    if not raw_json:
        return []
    data = json.loads(raw_json)
    if not isinstance(data, list):
        raise ValueError("EVAL_QUERIES_JSON must be a JSON list")
    out: list[EvalQuery] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        q = item.get("query")
        if not isinstance(q, str) or not q.strip():
            continue
        expected = item.get("expected_document_ids")
        if isinstance(expected, list):
            expected_ids = [str(x) for x in expected if str(x)]
        else:
            expected_ids = None
        out.append(EvalQuery(query=q.strip(), expected_document_ids=expected_ids))
    return out


def _hit_at_k(results: list[dict[str, Any]], expected: set[str], k: int) -> bool:
    for r in results[:k]:
        payload = r.get("payload") if isinstance(r, dict) else None
        doc_id = payload.get("document_id") if isinstance(payload, dict) else None
        if isinstance(doc_id, str) and doc_id in expected:
            return True
    return False


def _mrr(results: list[dict[str, Any]], expected: set[str]) -> float:
    for i, r in enumerate(results):
        payload = r.get("payload") if isinstance(r, dict) else None
        doc_id = payload.get("document_id") if isinstance(payload, dict) else None
        if isinstance(doc_id, str) and doc_id in expected:
            return 1.0 / (i + 1)
    return 0.0


@flow(name="eval_flow")
def eval_flow(
    pipeline_version: str | None = None,
    limit: int = 10,
) -> dict[str, object]:
    """
    Phase 7: nightly evaluation report.

    - Optional golden query set via `EVAL_QUERIES_JSON`
    - Stores report at: library/eval/<pipeline_version>/<YYYY-MM-DD>/report.json
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    today = datetime.now(UTC).date().isoformat()

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

    embedder = EmbeddingClient(
        base_url=settings.embedding_base_url,
        api_key=settings.embedding_api_key,
        max_batch_texts=settings.embedding_max_batch_texts,
        max_batch_chars=settings.embedding_max_batch_chars,
    )
    qdrant = QdrantClient(base_url=settings.qdrant_url, api_key=settings.qdrant_api_key)

    # Baseline counts.
    with connect(dsn, schema="public") as conn:
        doc_counts = conn.execute(
            "select status, count(*) from documents group by status order by status"
        ).fetchall()
        status_counts = {row[0]: int(row[1]) for row in doc_counts}

    queries = _load_queries(settings.eval_queries_json)
    query_results: list[dict[str, Any]] = []
    hits_at_1 = hits_at_3 = hits_at_10 = 0
    mrr_total = 0.0
    scored = 0

    for q in queries:
        vec = embedder.embed_texts([q.query])[0]
        results = qdrant.search(collection=settings.qdrant_collection, vector=vec, limit=limit)
        row: dict[str, Any] = {"query": q.query, "limit": limit, "results": results}

        if q.expected_document_ids:
            expected = set(q.expected_document_ids)
            row["expected_document_ids"] = list(expected)
            row["hit_at_1"] = _hit_at_k(results, expected, 1)
            row["hit_at_3"] = _hit_at_k(results, expected, 3)
            row["hit_at_10"] = _hit_at_k(results, expected, 10)
            row["mrr"] = _mrr(results, expected)
            hits_at_1 += 1 if row["hit_at_1"] else 0
            hits_at_3 += 1 if row["hit_at_3"] else 0
            hits_at_10 += 1 if row["hit_at_10"] else 0
            mrr_total += float(row["mrr"])
            scored += 1

        query_results.append(row)

    report: dict[str, Any] = {
        "pipeline_version": pv,
        "generated_at": datetime.now(UTC).isoformat(),
        "qdrant_collection": settings.qdrant_collection,
        "document_status_counts": status_counts,
        "golden_queries_count": len(queries),
        "golden_scored_count": scored,
        "metrics": {
            "hit_at_1": (hits_at_1 / scored) if scored else None,
            "hit_at_3": (hits_at_3 / scored) if scored else None,
            "hit_at_10": (hits_at_10 / scored) if scored else None,
            "mrr": (mrr_total / scored) if scored else None,
        },
        "queries": query_results,
    }

    key = f"library/eval/{pv}/{today}/report.json"
    uri = s3.put_bytes(key, json.dumps(report, indent=2, sort_keys=True).encode("utf-8"))
    logger.info("Wrote eval report: %s", uri)
    return {"ok": True, "report_uri": uri, "golden_queries": len(queries), "scored": scored}

