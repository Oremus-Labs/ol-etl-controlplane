from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import httpx
from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from prefect import flow, get_run_logger

from ol_etl_controlplane.config import load_settings

_HTML_TAG_RE = re.compile(r"<[^>]{2,}>")


def _alpha_ratio(s: str) -> float:
    if not s:
        return 0.0
    alpha = sum(ch.isalpha() for ch in s)
    return alpha / max(1, len(s))


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


def _qdrant_count(
    *,
    base_url: str,
    api_key: str | None,
    collection: str,
    document_id: str,
    pipeline_version: str,
) -> int:
    headers = {"api-key": api_key} if api_key else {}
    body = {
        "exact": True,
        "filter": {
            "must": [
                {"key": "document_id", "match": {"value": document_id}},
                {"key": "pipeline_version", "match": {"value": pipeline_version}},
            ]
        },
    }
    url = base_url.rstrip("/") + f"/collections/{collection}/points/count"
    with httpx.Client(timeout=20) as client:
        r = client.post(url, headers=headers, json=body)
        r.raise_for_status()
        return int((r.json().get("result") or {}).get("count") or 0)


def _qdrant_sample_payload(
    *,
    base_url: str,
    api_key: str | None,
    collection: str,
    document_id: str,
    pipeline_version: str,
) -> dict[str, Any] | None:
    headers = {"api-key": api_key} if api_key else {}
    body = {
        "limit": 1,
        "with_payload": True,
        "with_vector": False,
        "filter": {
            "must": [
                {"key": "document_id", "match": {"value": document_id}},
                {"key": "pipeline_version", "match": {"value": pipeline_version}},
            ]
        },
    }
    url = base_url.rstrip("/") + f"/collections/{collection}/points/scroll"
    with httpx.Client(timeout=20) as client:
        r = client.post(url, headers=headers, json=body)
        r.raise_for_status()
        points = ((r.json().get("result") or {}).get("points")) or []
        if not points:
            return None
        return points[0].get("payload") or None


@flow(name="quality_audit_flow")
def quality_audit_flow(
    *,
    pipeline_version: str | None = None,
    qdrant_collection: str | None = None,
    sources_csv: str | None = None,
    max_docs_per_source: int = 10,
    min_extract_chars: int = 200,
    max_html_tag_ratio: float = 0.02,
    save_report_to_minio: bool = True,
) -> dict[str, object]:
    """
    Runs a quality audit over recently-processed docs. This is the go/no-go gate before scaling.

    Checks:
    - Postgres status distribution and review queue open items
    - Empty/low-quality extracted_text artifacts
    - chunks table vs chunk jsonl consistency
    - Qdrant points count vs chunk rows for indexed_ok docs
    - Qdrant points present for docs not indexed_ok (orphan points)
    - Missing metadata fields needed for UI/search (title/canonical_url/published_year)
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    collection = qdrant_collection or settings.qdrant_collection

    dsn = _pg_dsn_from_env(settings)
    apply_migrations(dsn, schema="public")

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")

    s3 = S3Client(
        S3Config(
            endpoint=settings.s3_endpoint,
            bucket=settings.s3_bucket,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
        )
    )

    src_filter = None
    if sources_csv:
        src_filter = [s.strip() for s in sources_csv.split(",") if s.strip()]

    now = datetime.now(UTC)
    issues: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []

    with connect(dsn, schema="public") as conn:
        docs_repo = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)

        by_status = conn.execute(
            "select status, count(*) from documents group by status order by count(*) desc"
        ).fetchall()
        by_source = conn.execute(
            "select source, count(*) from documents group by source order by count(*) desc"
        ).fetchall()
        review_open = conn.execute(
            "select reason, count(*) from review_queue where status='open' "
            "group by reason order by count(*) desc"
        ).fetchall()

        sources = [r[0] for r in by_source]
        if src_filter is not None:
            sources = [s for s in sources if s in src_filter]

        for source in sources:
            rows = conn.execute(
                """
                select document_id
                from documents
                where source=%s
                order by created_at desc
                limit %s
                """,
                (source, max_docs_per_source),
            ).fetchall()
            for (document_id,) in rows:
                doc = docs_repo.get_document(document_id)
                if not doc:
                    continue

                if not doc.title:
                    issues.append(
                        {
                            "type": "missing_title",
                            "document_id": document_id,
                            "source": doc.source,
                            "status": doc.status,
                        }
                    )
                if not doc.canonical_url:
                    issues.append(
                        {
                            "type": "missing_canonical_url",
                            "document_id": document_id,
                            "source": doc.source,
                            "status": doc.status,
                        }
                    )
                if doc.source == "vatican_sqlite" and doc.published_year is None:
                    issues.append(
                        {
                            "type": "missing_published_year",
                            "document_id": document_id,
                            "source": doc.source,
                            "status": doc.status,
                        }
                    )

                q_count = _qdrant_count(
                    base_url=settings.qdrant_url,
                    api_key=settings.qdrant_api_key,
                    collection=collection,
                    document_id=document_id,
                    pipeline_version=pv,
                )
                if doc.status != "indexed_ok" and q_count > 0:
                    issues.append(
                        {
                            "type": "qdrant_orphan_points",
                            "document_id": document_id,
                            "status": doc.status,
                            "qdrant_points": q_count,
                        }
                    )
                if doc.status == "indexed_ok" and q_count == 0:
                    issues.append(
                        {
                            "type": "qdrant_missing_points",
                            "document_id": document_id,
                        }
                    )

                extracted = files_repo.get_file(document_id=document_id, variant="extracted_text")
                extracted_stats: dict[str, Any] | None = None
                if extracted:
                    text = s3.get_bytes_uri(extracted.storage_uri).decode("utf-8", errors="replace")
                    html_tags = len(_HTML_TAG_RE.findall(text))
                    extracted_stats = {
                        "chars": len(text),
                        "alpha_ratio": round(_alpha_ratio(text), 4),
                        "html_tag_count": html_tags,
                        "html_tag_ratio": (html_tags / max(1, len(text))),
                        "head": text[:250].replace("\n", "\\n"),
                    }
                    if len(text.strip()) < min_extract_chars:
                        issues.append(
                            {
                                "type": "extracted_too_short",
                                "document_id": document_id,
                                "chars": len(text),
                                "min_chars": min_extract_chars,
                            }
                        )
                    if extracted_stats["html_tag_ratio"] > max_html_tag_ratio:
                        issues.append(
                            {
                                "type": "extracted_contains_html",
                                "document_id": document_id,
                                "html_tag_ratio": extracted_stats["html_tag_ratio"],
                                "max_html_tag_ratio": max_html_tag_ratio,
                            }
                        )

                chunk_rows = conn.execute(
                    (
                        "select chunk_id, chunk_index, token_count, text_uri "
                        "from chunks where document_id=%s and pipeline_version=%s "
                        "order by chunk_index"
                    ),
                    (document_id, pv),
                ).fetchall()
                chunk_stats: dict[str, Any] | None = None
                if chunk_rows:
                    text_uri = chunk_rows[0][3]
                    try:
                        lines = (
                            s3.get_bytes_uri(text_uri)
                            .decode("utf-8", errors="replace")
                            .splitlines()
                        )
                        parsed = [json.loads(line) for line in lines if line.strip()]
                        chunk_stats = {
                            "db_count": len(chunk_rows),
                            "jsonl_count": len(parsed),
                            "text_uri": text_uri,
                        }
                        if len(parsed) != len(chunk_rows):
                            issues.append(
                                {
                                    "type": "chunk_jsonl_count_mismatch",
                                    "document_id": document_id,
                                    "db_count": len(chunk_rows),
                                    "jsonl_count": len(parsed),
                                    "text_uri": text_uri,
                                }
                            )
                    except Exception as e:  # noqa: BLE001
                        issues.append(
                            {
                                "type": "chunk_jsonl_parse_error",
                                "document_id": document_id,
                                "text_uri": text_uri,
                                "error": str(e),
                            }
                        )

                if doc.status == "indexed_ok":
                    db_chunk_count = len(chunk_rows)
                    if q_count != db_chunk_count:
                        issues.append(
                            {
                                "type": "qdrant_count_mismatch",
                                "document_id": document_id,
                                "db_chunks": db_chunk_count,
                                "qdrant_points": q_count,
                            }
                        )

                    payload = _qdrant_sample_payload(
                        base_url=settings.qdrant_url,
                        api_key=settings.qdrant_api_key,
                        collection=collection,
                        document_id=document_id,
                        pipeline_version=pv,
                    )
                    if payload is None:
                        issues.append(
                            {
                                "type": "qdrant_missing_payload",
                                "document_id": document_id,
                            }
                        )
                    if extracted is None and not doc.is_scanned:
                        issues.append(
                            {
                                "type": "indexed_without_extracted_text",
                                "document_id": document_id,
                            }
                        )

                samples.append(
                    {
                        "document_id": document_id,
                        "source": doc.source,
                        "status": doc.status,
                        "title": doc.title,
                        "qdrant_points": q_count,
                        "extracted": extracted_stats,
                        "chunks": chunk_stats,
                    }
                )

    report: dict[str, Any] = {
        "pipeline_version": pv,
        "qdrant_collection": collection,
        "generated_at": now.isoformat(),
        "documents_by_status": by_status,
        "documents_by_source": by_source,
        "review_open_by_reason": review_open,
        "issues": issues,
        "issues_count": len(issues),
        "samples": samples,
    }

    if save_report_to_minio:
        key = f"library/eval/{pv}/{now.date().isoformat()}/quality_audit.json"
        s3.put_bytes(
            key,
            json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"),
            content_type="application/json; charset=utf-8",
        )
        logger.info("Wrote audit report to s3://%s/%s", s3.bucket, key)

    logger.info("Quality audit complete: issues=%s", len(issues))
    return report
