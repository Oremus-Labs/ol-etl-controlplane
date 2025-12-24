#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import boto3
import httpx
import psycopg


VATICAN_NAV_PATTERNS = [
    re.compile(r"\bLa Santa Sede\b", re.IGNORECASE),
    re.compile(r"\bItaliano\b.*\bFrançais\b.*\bEnglish\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bMagisterium\b", re.IGNORECASE),
]


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"expected s3://... uri, got {uri!r}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


def _load_k8s_secret(namespace: str, name: str) -> dict[str, str]:
    raw = subprocess.check_output(
        ["kubectl", "-n", namespace, "get", "secret", name, "-o", "json"],
        text=True,
    )
    payload = json.loads(raw).get("data") or {}
    return {k: base64.b64decode(v).decode() for k, v in payload.items()}


@dataclass(frozen=True)
class PostgresCreds:
    host: str
    port: int
    db: str
    user: str
    password: str


def _pg_creds_from_k8s(port_forward_host: str, port_forward_port: int) -> PostgresCreds:
    secret = _load_k8s_secret("research", "postgres")
    return PostgresCreds(
        host=port_forward_host,
        port=port_forward_port,
        db=secret["POSTGRES_DB"],
        user=secret["POSTGRES_USER"],
        password=secret["POSTGRES_PASSWORD"],
    )


def _qdrant_api_key_from_k8s() -> str:
    secret = _load_k8s_secret("research", "qdrant")
    return secret["QDRANT_API_KEY"]


def _contains_vatican_nav(text: str) -> bool:
    return any(p.search(text) for p in VATICAN_NAV_PATTERNS)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Spot-check indexed docs for structural + content quality (Postgres + MinIO + Qdrant)."
    )
    ap.add_argument("--doc-ids-file", default="/tmp/spotcheck_doc_ids.txt")
    ap.add_argument("--pipeline-version", default=os.environ.get("PIPELINE_VERSION", "v1"))
    ap.add_argument("--qdrant-url", default="http://127.0.0.1:6333")
    ap.add_argument("--qdrant-collection", default="library_chunks_v1")
    ap.add_argument("--pg-host", default="127.0.0.1")
    ap.add_argument("--pg-port", type=int, default=5432)
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "https://s3.oremuslabs.app"))
    ap.add_argument("--out", default="/tmp/spotcheck_index_quality.json")
    args = ap.parse_args()

    now = datetime.now(timezone.utc).isoformat()
    pipeline_version = args.pipeline_version

    doc_ids: list[str] = []
    with open(args.doc_ids_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                doc_ids.append(line)
    if not doc_ids:
        raise SystemExit("no doc ids provided")

    pg = _pg_creds_from_k8s(args.pg_host, args.pg_port)
    qdrant_api_key = _qdrant_api_key_from_k8s()

    s3 = boto3.client("s3", endpoint_url=args.s3_endpoint)

    qdrant_headers = {"api-key": qdrant_api_key}
    qdrant = httpx.Client(timeout=30.0, headers=qdrant_headers)

    report: dict[str, Any] = {
        "generated_at": now,
        "pipeline_version": pipeline_version,
        "qdrant_collection": args.qdrant_collection,
        "doc_ids_file": args.doc_ids_file,
        "doc_ids_count": len(doc_ids),
        "results": [],
        "issues": [],
    }

    conn = psycopg.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.db,
        user=pg.user,
        password=pg.password,
    )
    cur = conn.cursor()

    for doc_id in doc_ids:
        cur.execute(
            "select source, title, canonical_url, published_year, language, status from documents where document_id=%s",
            (doc_id,),
        )
        doc_row = cur.fetchone()
        if not doc_row:
            report["issues"].append({"document_id": doc_id, "type": "missing_documents_row"})
            continue
        source, title, canonical_url, published_year, language, status = doc_row

        cur.execute(
            "select extracted_uri from extractions where document_id=%s and pipeline_version=%s order by created_at desc limit 1",
            (doc_id, pipeline_version),
        )
        ext_row = cur.fetchone()
        if not ext_row:
            report["issues"].append({"document_id": doc_id, "type": "missing_extraction_row"})
            continue
        extracted_uri = ext_row[0]

        extracted_bucket, extracted_key = _parse_s3_uri(extracted_uri)
        extracted_text = (
            s3.get_object(Bucket=extracted_bucket, Key=extracted_key)["Body"].read().decode("utf-8", "replace")
        )
        extracted_head = extracted_text[:600].replace("\n", "\\n")
        extracted_contains_nav = _contains_vatican_nav(extracted_text[:5000])

        cur.execute(
            "select count(*) from chunks where document_id=%s and pipeline_version=%s",
            (doc_id, pipeline_version),
        )
        chunks_count = cur.fetchone()[0]

        count_resp = qdrant.post(
            f"{args.qdrant_url.rstrip('/')}/collections/{args.qdrant_collection}/points/count",
            json={
                "exact": True,
                "filter": {
                    "must": [
                        {"key": "document_id", "match": {"value": doc_id}},
                        {"key": "pipeline_version", "match": {"value": pipeline_version}},
                    ]
                },
            },
        )
        count_resp.raise_for_status()
        qdrant_count = (count_resp.json().get("result") or {}).get("count")

        scroll_resp = qdrant.post(
            f"{args.qdrant_url.rstrip('/')}/collections/{args.qdrant_collection}/points/scroll",
            json={
                "limit": 1,
                "with_vector": False,
                "with_payload": True,
                "filter": {
                    "must": [
                        {"key": "document_id", "match": {"value": doc_id}},
                        {"key": "pipeline_version", "match": {"value": pipeline_version}},
                        {"key": "chunk_index", "match": {"value": 0}},
                    ]
                },
            },
        )
        scroll_resp.raise_for_status()
        points = (scroll_resp.json().get("result") or {}).get("points") or []
        qdrant_chunk0_text = ((points[0].get("payload") or {}).get("text") or "") if points else ""
        qdrant_chunk0_contains_nav = _contains_vatican_nav(qdrant_chunk0_text[:2000]) if qdrant_chunk0_text else None

        row_issues: list[str] = []
        for field, value in [
            ("title", title),
            ("canonical_url", canonical_url),
            ("published_year", published_year),
        ]:
            if value in (None, ""):
                row_issues.append(f"missing_{field}")

        if extracted_contains_nav:
            row_issues.append("extracted_contains_nav_boilerplate")
        if not extracted_text.strip():
            row_issues.append("extracted_empty")

        if chunks_count != qdrant_count:
            row_issues.append("chunks_qdrant_count_mismatch")

        if qdrant_chunk0_contains_nav is True:
            row_issues.append("qdrant_chunk0_contains_nav_boilerplate")

        report["results"].append(
            {
                "document_id": doc_id,
                "source": source,
                "status": status,
                "language": language,
                "title": title,
                "canonical_url": canonical_url,
                "published_year": published_year,
                "extracted_uri": extracted_uri,
                "extracted_chars": len(extracted_text),
                "extracted_head": extracted_head,
                "extracted_contains_nav_boilerplate": extracted_contains_nav,
                "chunks_count": chunks_count,
                "qdrant_points": qdrant_count,
                "qdrant_chunk0_text_head": qdrant_chunk0_text[:220].replace("\n", "\\n"),
                "qdrant_chunk0_contains_nav_boilerplate": qdrant_chunk0_contains_nav,
                "issues": row_issues,
            }
        )
        for issue in row_issues:
            report["issues"].append({"document_id": doc_id, "type": issue})

    report["issues_count"] = len(report["issues"])
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Wrote {args.out} (issues_count={report['issues_count']})")
    return 0 if report["issues_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

