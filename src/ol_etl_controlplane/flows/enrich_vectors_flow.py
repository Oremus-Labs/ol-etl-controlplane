from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.ocr.client import LlmServiceClient
from ol_rag_pipeline_core.qdrant import QdrantClient, deterministic_point_id
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.enrichments import ChunkEnrichmentRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from ol_rag_pipeline_core.util import sha256_bytes
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


def _parse_first_json_object(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:  # noqa: BLE001
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("LLM response was not valid JSON")
    payload = json.loads(text[start : end + 1])
    if not isinstance(payload, dict):
        raise RuntimeError("LLM response JSON was not an object")
    return payload


def _normalize_str_list(items: object, *, max_items: int) -> list[str]:
    if not isinstance(items, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= max_items:
            break
    return out


def _validate_and_normalize_payload(payload: dict[str, Any]) -> tuple[float | None, dict[str, Any]]:
    conf = payload.get("confidence")
    confidence: float | None
    if conf is None:
        confidence = None
    else:
        confidence = float(conf)
        if not (0.0 <= confidence <= 1.0):
            raise ValueError("confidence must be between 0 and 1")

    summary_raw = payload.get("summary")
    if summary_raw is not None and not isinstance(summary_raw, str):
        raise ValueError("summary must be a string")
    summary = summary_raw.strip() if isinstance(summary_raw, str) else ""

    keywords = _normalize_str_list(payload.get("keywords"), max_items=20)
    topics = _normalize_str_list(payload.get("topics"), max_items=10)

    entities_raw = payload.get("entities")
    entities: dict[str, list[str]] = {"persons": [], "orgs": [], "places": []}
    if isinstance(entities_raw, dict):
        entities["persons"] = _normalize_str_list(entities_raw.get("persons"), max_items=25)
        entities["orgs"] = _normalize_str_list(entities_raw.get("orgs"), max_items=25)
        entities["places"] = _normalize_str_list(entities_raw.get("places"), max_items=25)

    normalized: dict[str, Any] = {
        "confidence": confidence,
        "summary": summary,
        "keywords": keywords,
        "topics": topics,
        "entities": entities,
    }
    return confidence, normalized


def _build_enrichment_prompt(
    *,
    chunk_text: str,
    document_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    system = (
        "Return ONLY one JSON object (no markdown, no prose).\n"
        "Do not invent facts.\n"
        "If you are not highly confident, set confidence < 0.95 and keep fields minimal."
    )
    ctx = document_context or {}
    user = json.dumps(
        {
            "schema": {
                "confidence": "number 0..1",
                "summary": "string <= 60 words",
                "keywords": "string[] 0..20",
                "topics": "string[] 0..10",
                "entities": {"persons": "string[]", "orgs": "string[]", "places": "string[]"},
            },
            "document_context": ctx,
            "chunk_text": chunk_text,
        },
        ensure_ascii=False,
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


@flow(name="enrich_vectors_flow")
def enrich_vectors_flow(
    *,
    pipeline_version: str | None = None,
    enrichment_version: str = "gpt20b_v1",
    model: str = "gpt-20b",
    confidence_threshold: float = 0.95,
    source: str | None = None,
    limit_chunks: int = 200,
    include_rejected: bool = False,
    dry_run: bool = False,
    llm_max_tokens: int = 2048,
) -> dict[str, object]:
    """
    Day-2 enrichment:
    - Select already-indexed chunks missing enrichment (or stale when chunk text changed)
    - Call GPT-20B (via ol-llm-service) to generate structured metadata with confidence
    - Persist enrichment history in Postgres
    - If confidence >= threshold, update Qdrant payload (no re-embedding)

    This is intentionally not part of the initial ingest/index path.
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    now = datetime.now(UTC)

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
    qdrant = QdrantClient(base_url=settings.qdrant_url, api_key=settings.qdrant_api_key)
    llm = LlmServiceClient(
        base_url=settings.llm_service_url,
        api_key=settings.llm_service_api_key,
        timeout_s=900.0,
    )

    processed = 0
    accepted = 0
    errors = 0

    with connect(dsn, schema="public") as conn:
        docs_repo = DocumentRepository(conn)
        enrich_repo = ChunkEnrichmentRepository(conn)

        candidates = enrich_repo.list_candidates(
            pipeline_version=pv,
            enrichment_version=enrichment_version,
            source=source,
            limit=limit_chunks,
            include_rejected=include_rejected,
        )
        if not candidates:
            logger.info("No enrichment candidates found.")
            return {
                "pipeline_version": pv,
                "enrichment_version": enrichment_version,
                "processed": 0,
                "accepted": 0,
                "errors": 0,
                "dry_run": dry_run,
            }

        # Group by document so we only download each chunks.jsonl once.
        by_doc: dict[str, list] = {}
        for c in candidates:
            by_doc.setdefault(c.document_id, []).append(c)

        for document_id, doc_chunks in by_doc.items():
            text_uri = doc_chunks[0].text_uri
            if not text_uri:
                for c in doc_chunks:
                    enrich_repo.upsert(
                        chunk_id=c.chunk_id,
                        enrichment_version=enrichment_version,
                        model=model,
                        chunk_sha256=c.chunk_sha256 or "",
                        input_sha256=sha256_bytes(
                            f"{enrichment_version}:{c.chunk_sha256}".encode()
                        ),
                        confidence=None,
                        accepted=False,
                        output_json=None,
                        error="Missing chunks.text_uri",
                        applied_at=None,
                    )
                    errors += 1
                continue

            doc = docs_repo.get_document(document_id)
            doc_ctx: dict[str, Any] | None = None
            if doc:
                doc_ctx = {
                    "document_id": doc.document_id,
                    "source": doc.source,
                    "title": doc.title,
                    "author": doc.author,
                    "published_year": doc.published_year,
                    "language": doc.language,
                }

            lines = s3.get_bytes_uri(text_uri).decode("utf-8", errors="replace").splitlines()
            chunk_map: dict[int, dict[str, Any]] = {}
            for line in lines:
                if not line.strip():
                    continue
                row = json.loads(line)
                idx = row.get("chunk_index")
                if isinstance(idx, int):
                    chunk_map[idx] = row

            for c in doc_chunks:
                processed += 1
                chunk_sha = c.chunk_sha256 or ""
                input_sha = sha256_bytes(f"{enrichment_version}:{chunk_sha}".encode())

                row = chunk_map.get(c.chunk_index)
                if not row:
                    enrich_repo.upsert(
                        chunk_id=c.chunk_id,
                        enrichment_version=enrichment_version,
                        model=model,
                        chunk_sha256=chunk_sha,
                        input_sha256=input_sha,
                        confidence=None,
                        accepted=False,
                        output_json=None,
                        error=f"Missing chunk_index={c.chunk_index} in chunks jsonl",
                        applied_at=None,
                    )
                    errors += 1
                    continue

                chunk_text = row.get("text")
                if not isinstance(chunk_text, str) or not chunk_text.strip():
                    enrich_repo.upsert(
                        chunk_id=c.chunk_id,
                        enrichment_version=enrichment_version,
                        model=model,
                        chunk_sha256=chunk_sha,
                        input_sha256=input_sha,
                        confidence=None,
                        accepted=False,
                        output_json=None,
                        error="Empty chunk text",
                        applied_at=None,
                    )
                    errors += 1
                    continue

                try:
                    messages = _build_enrichment_prompt(
                        chunk_text=chunk_text,
                        document_context=doc_ctx,
                    )
                    content = llm.chat_completion(
                        model=model,
                        messages=messages,
                        max_tokens=llm_max_tokens,
                        temperature=0.0,
                    )
                    payload = _parse_first_json_object(content)
                    confidence, payload = _validate_and_normalize_payload(payload)
                    is_accepted = bool(
                        confidence is not None and confidence >= confidence_threshold
                    )

                    applied_at = None
                    if is_accepted and not dry_run:
                        qdrant.set_payload(
                            collection=settings.qdrant_collection,
                            point_ids=[str(deterministic_point_id(chunk_id=c.chunk_id))],
                            payload={
                                "enrichment": {
                                    "version": enrichment_version,
                                    "model": model,
                                    "confidence": confidence,
                                    "summary": payload["summary"],
                                    "keywords": payload["keywords"],
                                    "topics": payload["topics"],
                                    "entities": payload["entities"],
                                    "enriched_at": now.isoformat(),
                                }
                            },
                        )
                        applied_at = now

                    enrich_repo.upsert(
                        chunk_id=c.chunk_id,
                        enrichment_version=enrichment_version,
                        model=model,
                        chunk_sha256=chunk_sha,
                        input_sha256=input_sha,
                        confidence=confidence,
                        accepted=is_accepted,
                        output_json=payload,
                        error=None,
                        applied_at=applied_at,
                    )
                    if is_accepted:
                        accepted += 1
                except Exception as e:  # noqa: BLE001
                    enrich_repo.upsert(
                        chunk_id=c.chunk_id,
                        enrichment_version=enrichment_version,
                        model=model,
                        chunk_sha256=chunk_sha,
                        input_sha256=input_sha,
                        confidence=None,
                        accepted=False,
                        output_json=None,
                        error=str(e),
                        applied_at=None,
                    )
                    errors += 1

    logger.info(
        (
            "Enrichment complete: pipeline_version=%s enrichment_version=%s processed=%s "
            "accepted=%s errors=%s dry_run=%s"
        ),
        pv,
        enrichment_version,
        processed,
        accepted,
        errors,
        dry_run,
    )
    return {
        "pipeline_version": pv,
        "enrichment_version": enrichment_version,
        "processed": processed,
        "accepted": accepted,
        "errors": errors,
        "dry_run": dry_run,
    }
