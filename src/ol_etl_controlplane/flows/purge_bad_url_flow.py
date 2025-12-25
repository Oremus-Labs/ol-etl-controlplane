from __future__ import annotations

from dataclasses import dataclass

from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.qdrant import QdrantClient
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.storage.s3 import S3Client, S3Config, parse_s3_uri
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
class PurgePlan:
    document_id: str
    source: str
    source_uri: str
    canonical_url: str | None
    pipeline_versions: list[str]
    s3_uris: list[str]


def _list_pipeline_versions_for_document(conn, *, document_id: str) -> list[str]:  # noqa: ANN001
    rows = conn.execute(
        """
        select distinct pipeline_version
        from chunks
        where document_id=%s
        order by pipeline_version
        """,
        (document_id,),
    ).fetchall()
    return [r[0] for r in rows]


def _list_s3_uris_for_document(conn, *, document_id: str) -> list[str]:  # noqa: ANN001
    uris: list[str] = []

    file_rows = conn.execute(
        """
        select storage_uri
        from document_files
        where document_id=%s
        """,
        (document_id,),
    ).fetchall()
    uris.extend([r[0] for r in file_rows if r and r[0]])

    extraction_rows = conn.execute(
        """
        select extracted_uri
        from extractions
        where document_id=%s
        """,
        (document_id,),
    ).fetchall()
    uris.extend([r[0] for r in extraction_rows if r and r[0]])

    chunk_rows = conn.execute(
        """
        select text_uri
        from chunks
        where document_id=%s
        """,
        (document_id,),
    ).fetchall()
    uris.extend([r[0] for r in chunk_rows if r and r[0]])

    # Dedupe while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for u in uris:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


@flow(name="purge_bad_url_flow")
def purge_bad_url_flow(
    *,
    bad_url: str = "https://archive.org/details/the-complete-ante-nicene-nicene-and-post-nicene-church-fathers",
    source: str = "vatican_sqlite",
    dry_run: bool = True,
) -> dict[str, object]:
    """
    Deletes documents matching an EXACT bad URL (source_uri or canonical_url).

    Safety:
    - Exact match only (no wildcard)
    - Defaults to dry_run=True
    - Deletes only S3 objects in the configured S3_BUCKET (rag-artifacts), and only those
      URIs referenced by this document's DB rows.
    """
    settings = load_settings()
    logger = get_run_logger()

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

    qdrant = QdrantClient(base_url=settings.qdrant_url, api_key=settings.qdrant_api_key)

    plans: list[PurgePlan] = []
    with connect(dsn, schema="public") as conn:
        docs_repo = DocumentRepository(conn)
        rows = conn.execute(
            """
            select document_id
            from documents
            where source=%s
              and (source_uri=%s or canonical_url=%s)
            order by document_id
            """,
            (source, bad_url, bad_url),
        ).fetchall()
        doc_ids = [r[0] for r in rows]

        for document_id in doc_ids:
            doc = docs_repo.get_document(document_id)
            if not doc:
                continue
            pv = _list_pipeline_versions_for_document(conn, document_id=document_id)
            uris = _list_s3_uris_for_document(conn, document_id=document_id)
            plans.append(
                PurgePlan(
                    document_id=document_id,
                    source=doc.source,
                    source_uri=doc.source_uri,
                    canonical_url=doc.canonical_url,
                    pipeline_versions=pv,
                    s3_uris=uris,
                )
            )

    deleted_docs = 0
    deleted_s3_objects = 0
    deleted_qdrant_versions = 0

    for plan in plans:
        logger.info(
            "Purge candidate: document_id=%s source=%s source_uri=%s",
            plan.document_id,
            plan.source,
            plan.source_uri,
        )

        for uri in plan.s3_uris:
            if not uri.startswith("s3://"):
                continue
            bucket, key = parse_s3_uri(uri)
            if bucket != settings.s3_bucket:
                continue
            if dry_run:
                continue
            s3.delete_key(key)
            deleted_s3_objects += 1

        for pv in plan.pipeline_versions:
            if dry_run:
                continue
            qdrant.delete_points_for_document(
                collection=settings.qdrant_collection,
                document_id=plan.document_id,
                pipeline_version=pv,
            )
            deleted_qdrant_versions += 1

        if not dry_run:
            with connect(dsn, schema="public") as conn:
                conn.execute("delete from documents where document_id=%s", (plan.document_id,))
                conn.commit()
            deleted_docs += 1

    return {
        "dry_run": dry_run,
        "bad_url": bad_url,
        "source": source,
        "matched_documents": len(plans),
        "deleted_docs": deleted_docs,
        "deleted_s3_objects": deleted_s3_objects,
        "deleted_qdrant_versions": deleted_qdrant_versions,
        "candidates": [
            {
                "document_id": p.document_id,
                "source_uri": p.source_uri,
                "canonical_url": p.canonical_url,
                "pipeline_versions": p.pipeline_versions,
                "s3_uris_count": len(p.s3_uris),
            }
            for p in plans
        ],
    }

