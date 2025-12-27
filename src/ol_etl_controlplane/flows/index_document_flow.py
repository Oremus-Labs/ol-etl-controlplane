from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

from ol_rag_pipeline_core.chunking import chunk_pages, chunk_text
from ol_rag_pipeline_core.db import PostgresConfig, connect
from ol_rag_pipeline_core.embedding import EmbeddingClient
from ol_rag_pipeline_core.migrations.runner import apply_migrations
from ol_rag_pipeline_core.qdrant import QdrantClient, deterministic_point_id
from ol_rag_pipeline_core.repositories.chunks import ChunkRepository
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.files import DocumentFileRepository
from ol_rag_pipeline_core.repositories.ocr import OcrRepository
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


@flow(name="index_document_flow", retries=3, retry_delay_seconds=60)
def index_document_flow(
    document_id: str,
    pipeline_version: str | None = None,
    content_fingerprint: str | None = None,
    source: str | None = None,
    event_id: str | None = None,
    event: dict | None = None,
) -> dict[str, object]:
    """
    Phase 5:
    - Load extracted text from MinIO
    - Chunk with deterministic IDs
    - Embed via embedding endpoint
    - Replace-all upsert into Qdrant (no stale points)
    - Persist chunk rows and update document status indexed_ok
    """
    settings = load_settings()
    logger = get_run_logger()

    pv = pipeline_version or settings.pipeline_version
    correlation_id = event_id or str(uuid4())

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY")
    if not settings.embedding_base_url:
        raise RuntimeError("Missing EMBEDDING_BASE_URL")

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
    qdrant = QdrantClient(
        base_url=settings.qdrant_url,
        api_key=settings.qdrant_api_key,
    )

    with connect(dsn, schema="public") as conn:
        docs = DocumentRepository(conn)
        files_repo = DocumentFileRepository(conn)
        chunks_repo = ChunkRepository(conn)

        doc = docs.get_document(document_id)
        if not doc:
            raise RuntimeError(f"Document not found: {document_id}")

        if doc.status == "needs_review":
            raise RuntimeError("Cannot index needs_review docs; resolve review_queue first.")

        if (
            doc.content_fingerprint
            and content_fingerprint
            and doc.content_fingerprint != content_fingerprint
        ):
            logger.warning(
                "Fingerprint mismatch: document_id=%s db=%s param=%s",
                document_id,
                doc.content_fingerprint,
                content_fingerprint,
            )

        if doc.is_scanned:
            ocr_repo = OcrRepository(conn)
            run = ocr_repo.get_latest_run_for_document(
                document_id=document_id,
                pipeline_version=pv,
                status="complete",
            )
            if not run:
                raise RuntimeError(
                    "Scanned doc has no completed OCR run; run ocr_document_flow first."
                )
            pages = ocr_repo.list_pages(ocr_run_id=run.ocr_run_id)
            if not pages:
                raise RuntimeError("OCR run has no ocr_pages rows.")
            page_texts: list[tuple[int, str]] = []
            for p in pages:
                if not p.consensus_uri:
                    raise RuntimeError(
                        f"OCR page missing consensus_uri: page_number={p.page_number}"
                    )
                txt = s3.get_bytes_uri(p.consensus_uri).decode("utf-8", errors="replace")
                page_texts.append((p.page_number, txt))
            chunks = chunk_pages(
                pages=page_texts,
                max_tokens=settings.chunk_max_tokens,
                overlap_tokens=settings.chunk_overlap_tokens,
            )
        else:
            extracted_file = files_repo.get_file(document_id=document_id, variant="extracted_text")
            if not extracted_file:
                raise RuntimeError(f"Extracted text file not found for document: {document_id}")

            extracted_bytes = s3.get_bytes_uri(extracted_file.storage_uri)
            extracted_text = extracted_bytes.decode("utf-8", errors="replace")

            chunks = chunk_text(
                text=extracted_text,
                max_tokens=settings.chunk_max_tokens,
                overlap_tokens=settings.chunk_overlap_tokens,
            )
        if not chunks:
            raise RuntimeError("No chunks produced from extracted text.")

        # Store per-doc chunk jsonl (Plan contract).
        jsonl_lines: list[str] = []
        for i, ch in enumerate(chunks):
            row = {
                "chunk_index": i,
                "text": ch.text,
                "token_count": ch.token_count,
                "section_path": ch.section_path,
                "page_start": ch.page_start,
                "page_end": ch.page_end,
                "locator": ch.locator,
            }
            jsonl_lines.append(json.dumps(row, ensure_ascii=False))

        chunks_key = f"library/chunks/{pv}/{doc.source}/{document_id}.jsonl"
        chunks_uri = s3.put_bytes(
            chunks_key,
            ("\n".join(jsonl_lines) + "\n").encode("utf-8"),
            content_type="application/x-ndjson; charset=utf-8",
        )

        # Replace-all chunk rows.
        model_chunks = []
        for i, ch in enumerate(chunks):
            chunk_id = f"{document_id}:{pv}:{i}"
            model_chunks.append(
                {
                    "document_id": document_id,
                    "pipeline_version": pv,
                    "chunk_id": chunk_id,
                    "chunk_index": i,
                    "section_path": ch.section_path,
                    "token_count": ch.token_count,
                    "sha256": sha256_bytes(ch.text.encode("utf-8")),
                    "text_uri": chunks_uri,
                    "page_start": ch.page_start,
                    "page_end": ch.page_end,
                    "locator": ch.locator,
                }
            )

        # ChunkRepository expects Chunk model; avoid importing in controlplane by using
        # dict->dataclass mapping here.
        from ol_rag_pipeline_core.models import Chunk  # local import to keep flow import light

        chunks_repo.replace_chunks(
            document_id=document_id,
            pipeline_version=pv,
            chunks=[Chunk(**c) for c in model_chunks],
        )

        categories = docs.list_categories(document_id)

        # Embed and upsert to Qdrant with delete-by-filter semantics.
        embeddings = embedder.embed_texts([c.text for c in chunks])
        vector_size = len(embeddings[0])

        qdrant.ensure_collection(
            name=settings.qdrant_collection,
            vector_size=vector_size,
            distance="Cosine",
        )

        qdrant.delete_points_for_document(
            collection=settings.qdrant_collection,
            document_id=document_id,
            pipeline_version=pv,
        )

        points = []
        for i, (ch, vec) in enumerate(zip(chunks, embeddings, strict=True)):
            chunk_id = f"{document_id}:{pv}:{i}"
            payload = {
                # Identity (mandatory)
                "document_id": document_id,
                "chunk_id": chunk_id,
                "source": doc.source,
                "pipeline_version": pv,
                "fingerprint": doc.content_fingerprint,
                # Retrieval + citations (mandatory)
                "source_uri": doc.source_uri,
                "citation_anchor": None,
                "locator": ch.locator,
                # Structure (mandatory)
                "title": doc.title,
                "author": doc.author,
                "year": doc.published_year,
                "language": doc.language,
                "section_path": ch.section_path,
                # Additional useful fields
                "chunk_index": i,
                "token_count": ch.token_count,
                "categories": categories,
                "page_start": ch.page_start,
                "page_end": ch.page_end,
                "text": ch.text,
                "indexed_at": datetime.now(UTC).isoformat(),
                "correlation_id": correlation_id,
                "event": event,
            }
            points.append(
                {
                    "id": str(deterministic_point_id(chunk_id=chunk_id)),
                    "vector": vec,
                    "payload": payload,
                }
            )
        batch_size = settings.qdrant_upsert_batch_size
        if batch_size <= 0:
            batch_size = len(points)
        for i in range(0, len(points), batch_size):
            qdrant.upsert_points(
                collection=settings.qdrant_collection,
                points=points[i : i + batch_size],
            )

        # Preserve `is_scanned`: it describes the original input, not whether OCR has completed.
        docs.set_processing_state(
            document_id=document_id, status="indexed_ok", is_scanned=doc.is_scanned
        )
        logger.info(
            "Indexed: document_id=%s chunks=%s qdrant_collection=%s",
            document_id,
            len(points),
            settings.qdrant_collection,
        )
        return {
            "document_id": document_id,
            "pipeline_version": pv,
            "status": "indexed_ok",
            "chunks": len(points),
            "vector_size": vector_size,
            "qdrant_collection": settings.qdrant_collection,
        }
