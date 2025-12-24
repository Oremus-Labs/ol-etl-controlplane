from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import httpx
from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.qdrant import QdrantClient
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config
from prefect import flow, get_run_logger

from ol_etl_controlplane.config import load_settings

_META_REFRESH_RE = re.compile(r"http-equiv\s*=\s*([\"'])refresh\1", re.IGNORECASE)


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


@flow(name="reconcile_index_flow")
def reconcile_index_flow(
    *,
    pipeline_version: str | None = None,
    qdrant_collection: str | None = None,
    max_docs: int = 2000,
    sources_csv: str | None = None,
    dry_run: bool = True,
    delete_dotfiles: bool = True,
    delete_test_docs: bool = True,
    delete_meta_refresh_stubs: bool = True,
) -> dict[str, object]:
    """
    Reconciles Postgres/Qdrant drift:
    - If Qdrant has points but Postgres status isn't indexed_ok:
      - Promote to indexed_ok only when Postgres has chunk rows + extracted_text
        (or is_scanned=true)
      - Otherwise delete Qdrant points (treat as stale/orphan)
    - Optionally delete obvious dotfile docs (e.g. .DS_Store) from Postgres

    Intended as an operator "repair" tool to restore invariants.
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    collection = qdrant_collection or settings.qdrant_collection

    dsn = _pg_dsn_from_env(settings)
    apply_migrations(dsn, schema="public")

    qdrant = QdrantClient(base_url=settings.qdrant_url, api_key=settings.qdrant_api_key)
    s3: S3Client | None = None
    if settings.s3_access_key and settings.s3_secret_key:
        s3 = S3Client(
            S3Config(
                endpoint=settings.s3_endpoint,
                bucket=settings.s3_bucket,
                access_key=settings.s3_access_key,
                secret_key=settings.s3_secret_key,
            )
        )

    actions: list[dict[str, Any]] = []
    promoted = 0
    deleted_points = 0
    deleted_docs = 0

    source_filter = None
    if sources_csv:
        source_filter = {s.strip() for s in sources_csv.split(",") if s.strip()}

    with connect(dsn, schema="public") as conn:
        docs_repo = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)

        if source_filter:
            rows = conn.execute(
                """
                select document_id, source, source_uri, status, is_scanned
                from documents
                where source = any(%s::text[])
                order by updated_at desc
                limit %s
                """,
                (list(source_filter), max_docs),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                select document_id, source, source_uri, status, is_scanned
                from documents
                order by updated_at desc
                limit %s
                """,
                (max_docs,),
            ).fetchall()

        for document_id, source, source_uri, status, is_scanned in rows:
            source_uri_l = source_uri.lower() if isinstance(source_uri, str) else ""
            is_dotfile = (
                ".ds_store" in source_uri_l
                or "/__macosx" in source_uri_l
                or source_uri_l.endswith("/__macosx")
            )

            is_test_doc = (
                isinstance(document_id, str)
                and (
                    document_id.startswith("nextcloud:ocr_smoke_")
                    or document_id.startswith("nextcloud:_codex_")
                )
            )

            if delete_test_docs and is_test_doc:
                q_count = _qdrant_count(
                    base_url=settings.qdrant_url,
                    api_key=settings.qdrant_api_key,
                    collection=collection,
                    document_id=document_id,
                    pipeline_version=pv,
                )
                if q_count > 0:
                    actions.append(
                        {
                            "action": "delete_qdrant_points",
                            "document_id": document_id,
                            "reason": "test_doc",
                            "qdrant_points": q_count,
                        }
                    )
                actions.append(
                    {
                        "action": "delete_document",
                        "document_id": document_id,
                        "reason": "test_doc",
                    }
                )
                if not dry_run:
                    if q_count > 0:
                        qdrant.delete_points_for_document(
                            collection=collection,
                            document_id=document_id,
                            pipeline_version=pv,
                        )
                    conn.execute("delete from documents where document_id=%s", (document_id,))
                    conn.commit()
                if q_count > 0:
                    deleted_points += 1
                deleted_docs += 1
                continue

            if (
                delete_meta_refresh_stubs
                and source == "newadvent_zip"
                and source_uri_l.endswith("000.htm")
                and s3 is not None
            ):
                extracted = files_repo.get_file(document_id=document_id, variant="extracted_text")
                extracted_len = 0
                if extracted:
                    try:
                        extracted_text = s3.get_bytes_uri(extracted.storage_uri).decode(
                            "utf-8", errors="replace"
                        )
                        extracted_len = len(extracted_text.strip())
                    except Exception:  # noqa: BLE001
                        extracted_len = 0
                if extracted_len == 0:
                    raw = files_repo.get_file(document_id=document_id, variant="raw")
                    if raw:
                        raw_bytes = s3.get_bytes_uri(raw.storage_uri)
                        if len(raw_bytes) <= 4096 and _META_REFRESH_RE.search(
                            raw_bytes.decode("utf-8", errors="replace")
                        ):
                            q_count = _qdrant_count(
                                base_url=settings.qdrant_url,
                                api_key=settings.qdrant_api_key,
                                collection=collection,
                                document_id=document_id,
                                pipeline_version=pv,
                            )
                            actions.append(
                                {
                                    "action": "delete_document",
                                    "document_id": document_id,
                                    "reason": "meta_refresh_stub",
                                }
                            )
                            if q_count > 0:
                                actions.append(
                                    {
                                        "action": "delete_qdrant_points",
                                        "document_id": document_id,
                                        "reason": "meta_refresh_stub",
                                        "qdrant_points": q_count,
                                    }
                                )
                            if not dry_run:
                                if q_count > 0:
                                    qdrant.delete_points_for_document(
                                        collection=collection,
                                        document_id=document_id,
                                        pipeline_version=pv,
                                    )
                                conn.execute(
                                    "delete from documents where document_id=%s",
                                    (document_id,),
                                )
                                conn.commit()
                            if q_count > 0:
                                deleted_points += 1
                            deleted_docs += 1
                            continue
            if delete_dotfiles and is_dotfile:
                q_count = _qdrant_count(
                    base_url=settings.qdrant_url,
                    api_key=settings.qdrant_api_key,
                    collection=collection,
                    document_id=document_id,
                    pipeline_version=pv,
                )
                if q_count > 0:
                    actions.append(
                        {
                            "action": "delete_qdrant_points",
                            "document_id": document_id,
                            "reason": "dotfile",
                            "qdrant_points": q_count,
                        }
                    )
                actions.append(
                    {
                        "action": "delete_document",
                        "document_id": document_id,
                        "reason": "dotfile",
                    }
                )
                if not dry_run:
                    if q_count > 0:
                        qdrant.delete_points_for_document(
                            collection=collection,
                            document_id=document_id,
                            pipeline_version=pv,
                        )
                    conn.execute("delete from documents where document_id=%s", (document_id,))
                    conn.commit()
                if q_count > 0:
                    deleted_points += 1
                deleted_docs += 1
                continue

            q_count = _qdrant_count(
                base_url=settings.qdrant_url,
                api_key=settings.qdrant_api_key,
                collection=collection,
                document_id=document_id,
                pipeline_version=pv,
            )
            if q_count == 0:
                continue

            if status == "indexed_ok":
                continue

            chunk_count = conn.execute(
                "select count(*) from chunks where document_id=%s and pipeline_version=%s",
                (document_id, pv),
            ).fetchone()[0]
            extracted = files_repo.get_file(document_id=document_id, variant="extracted_text")

            can_promote = bool(chunk_count > 0 and (extracted is not None or bool(is_scanned)))

            if can_promote:
                actions.append(
                    {
                        "action": "promote_to_indexed_ok",
                        "document_id": document_id,
                        "previous_status": status,
                        "qdrant_points": q_count,
                        "chunks": chunk_count,
                    }
                )
                if not dry_run:
                    doc = docs_repo.get_document(document_id)
                    if doc:
                        docs_repo.set_processing_state(
                            document_id=document_id,
                            status="indexed_ok",
                            is_scanned=doc.is_scanned,
                        )
                promoted += 1
                continue

            actions.append(
                {
                    "action": "delete_qdrant_points",
                    "document_id": document_id,
                    "status": status,
                    "qdrant_points": q_count,
                    "chunks": chunk_count,
                    "has_extracted_text": extracted is not None,
                }
            )
            if not dry_run:
                qdrant.delete_points_for_document(
                    collection=collection,
                    document_id=document_id,
                    pipeline_version=pv,
                )
            deleted_points += 1

    logger.info(
        "Reconcile complete: promoted=%s deleted_points=%s deleted_docs=%s dry_run=%s",
        promoted,
        deleted_points,
        deleted_docs,
        dry_run,
    )
    return {
        "pipeline_version": pv,
        "qdrant_collection": collection,
        "sources_csv": sources_csv,
        "dry_run": dry_run,
        "promoted": promoted,
        "deleted_qdrant_point_sets": deleted_points,
        "deleted_documents": deleted_docs,
        "actions": actions,
        "generated_at": datetime.now(UTC).isoformat(),
    }
