from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_version: str = Field(alias="PIPELINE_VERSION")
    dataset_version: str = Field(alias="DATASET_VERSION")

    prefect_api_url: str = Field(alias="PREFECT_API_URL")

    pg_dsn: str | None = Field(default=None, alias="PG_DSN")
    postgres_host: str | None = Field(default=None, alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str | None = Field(default=None, alias="POSTGRES_DB")
    postgres_user: str | None = Field(default=None, alias="POSTGRES_USER")
    postgres_password: str | None = Field(default=None, alias="POSTGRES_PASSWORD")

    qdrant_url: str = Field(alias="QDRANT_URL")
    qdrant_api_key: str | None = Field(default=None, alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(default="library_chunks_v1", alias="QDRANT_COLLECTION")

    embedding_base_url: str | None = Field(default=None, alias="EMBEDDING_BASE_URL")
    embedding_api_key: str | None = Field(default=None, alias="EMBEDDING_API_KEY")
    embedding_max_batch_texts: int = Field(default=16, alias="EMBEDDING_MAX_BATCH_TEXTS")
    embedding_max_batch_chars: int = Field(default=12000, alias="EMBEDDING_MAX_BATCH_CHARS")

    s3_endpoint: str = Field(alias="S3_ENDPOINT")
    s3_bucket: str = Field(alias="S3_BUCKET")
    s3_access_key: str | None = Field(default=None, alias="S3_ACCESS_KEY")
    s3_secret_key: str | None = Field(default=None, alias="S3_SECRET_KEY")

    calibre_export_enabled: bool = Field(default=True, alias="CALIBRE_EXPORT_ENABLED")
    calibre_s3_bucket: str = Field(default="calibre-inbox", alias="CALIBRE_S3_BUCKET")
    calibre_s3_prefix: str = Field(default="etl", alias="CALIBRE_S3_PREFIX")
    calibre_import_enabled: bool = Field(default=True, alias="CALIBRE_IMPORT_ENABLED")
    calibre_importer_url: str = Field(
        default="http://calibre-importer.calibre-web.svc.cluster.local:8091",
        alias="CALIBRE_IMPORTER_URL",
    )
    calibre_importer_api_key: str | None = Field(default=None, alias="CALIBRE_IMPORTER_API_KEY")
    calibre_import_timeout_s: float = Field(default=60.0, alias="CALIBRE_IMPORT_TIMEOUT_S")

    nextcloud_webdav_url: str | None = Field(default=None, alias="NEXTCLOUD_WEBDAV_URL")
    nextcloud_user: str | None = Field(default=None, alias="NEXTCLOUD_USER")
    nextcloud_app_password: str | None = Field(default=None, alias="NEXTCLOUD_APP_PASSWORD")
    nextcloud_ingest_path: str = Field(default="ETL/Ingest", alias="NEXTCLOUD_INGEST_PATH")

    nats_url: str | None = Field(default=None, alias="NATS_URL")
    nats_subject: str = Field(default="docs.discovered", alias="NATS_SUBJECT")

    nextcloud_max_files: int = Field(default=25, alias="NEXTCLOUD_MAX_FILES")
    newadvent_seed_urls: str | None = Field(default=None, alias="NEWADVENT_SEED_URLS")
    newadvent_web_max_pages: int = Field(default=25, alias="NEWADVENT_WEB_MAX_PAGES")
    vatican_sqlite_max_rows: int = Field(default=25, alias="VATICAN_SQLITE_MAX_ROWS")
    vatican_sqlite_hosts: str | None = Field(
        default=None,
        alias="VATICAN_SQLITE_HOSTS",
        description="Comma-separated hostnames to include (e.g. www.vatican.va,archive.org).",
    )
    vatican_sqlite_sample_per_host: int | None = Field(
        default=None,
        alias="VATICAN_SQLITE_SAMPLE_PER_HOST",
        description=(
            "If set (and VATICAN_SQLITE_HOSTS is set), sample up to N rows per host "
            "before applying VATICAN_SQLITE_MAX_ROWS."
        ),
    )
    vatican_sqlite_exclude_urls: str | None = Field(
        default=None,
        alias="VATICAN_SQLITE_EXCLUDE_URLS",
        description="Comma-separated exact URLs to skip during Vatican sqlite sync.",
    )
    vatican_sqlite_exclude_prefixes: str | None = Field(
        default=None,
        alias="VATICAN_SQLITE_EXCLUDE_PREFIXES",
        description="Comma-separated URL prefixes to skip during Vatican sqlite sync.",
    )
    newadvent_zip_max_entries: int = Field(default=50, alias="NEWADVENT_ZIP_MAX_ENTRIES")
    newadvent_zip_include_prefixes: str | None = Field(
        default=None,
        alias="NEWADVENT_ZIP_INCLUDE_PREFIXES",
        description="Comma-separated ZIP top-level prefixes to include (e.g. cathen,fathers,bible).",
    )

    extract_min_chars: int = Field(default=200, alias="EXTRACT_MIN_CHARS")
    extract_min_alpha_ratio: float = Field(default=0.15, alias="EXTRACT_MIN_ALPHA_RATIO")

    chunk_max_tokens: int = Field(default=500, alias="CHUNK_MAX_TOKENS")
    chunk_overlap_tokens: int = Field(default=50, alias="CHUNK_OVERLAP_TOKENS")

    llm_service_url: str = Field(default="http://10.10.10.8:8090", alias="LLM_SERVICE_URL")
    llm_service_api_key: str | None = Field(default=None, alias="LLM_SERVICE_API_KEY")
    ocr_engines: str = Field(
        default="mineru,monkeyocr_pro,qwen3_vl",
        alias="OCR_ENGINES",
        description=(
            "Comma-separated ol-llm-service OCR engine names "
            "(e.g. mineru,monkeyocr_pro,qwen3_vl)."
        ),
    )
    ocr_pdf_dpi: int = Field(default=200, alias="OCR_PDF_DPI")
    ocr_pdf_max_pages: int | None = Field(default=None, alias="OCR_PDF_MAX_PAGES")

    ocr_min_chars_per_page: int = Field(default=40, alias="OCR_MIN_CHARS_PER_PAGE")
    ocr_min_alpha_ratio: float = Field(default=0.10, alias="OCR_MIN_ALPHA_RATIO")
    ocr_min_printable_ratio: float = Field(default=0.85, alias="OCR_MIN_PRINTABLE_RATIO")

    eval_queries_json: str | None = Field(
        default=None,
        alias="EVAL_QUERIES_JSON",
        description=(
            "JSON list of eval queries with optional expected document ids; "
            "written to MinIO report."
        ),
    )

    # VPN safety/rotation (for high-volume external crawls)
    gluetun_control_url: str = Field(default="http://127.0.0.1:8000", alias="GLUETUN_CONTROL_URL")
    gluetun_api_key: str | None = Field(default=None, alias="GLUETUN_API_KEY")
    vpn_required: bool = Field(
        default=True,
        alias="VPN_REQUIRED",
        description=(
            "If true, external HTTP(S) fetches require an active VPN (Gluetun) "
            "or they fail fast."
        ),
    )
    vpn_rotate_every_n_requests: int = Field(
        default=10,
        alias="VPN_ROTATE_EVERY_N_REQUESTS",
        description="Rotate the VPN connection every N external HTTP(S) requests.",
    )
    vpn_ensure_timeout_s: float = Field(default=90.0, alias="VPN_ENSURE_TIMEOUT_S")
    vpn_rotate_cooldown_s: float = Field(default=2.0, alias="VPN_ROTATE_COOLDOWN_S")
    vpn_http_proxy_pool: str | None = Field(
        default=None,
        alias="VPN_HTTP_PROXY_POOL",
        description=(
            "Comma-separated HTTP CONNECT proxies for external crawls. "
            "When set, flows rotate between these proxies (instead of restarting OpenVPN). "
            "Example: http://gluetun-egress-nl.research.svc.cluster.local:8888,http://gluetun-egress-de.research.svc.cluster.local:8888"
        ),
    )

    # Vatican fetch policy (speed vs completeness trade-offs)
    vatican_http_max_attempts: int = Field(
        default=1,
        alias="VATICAN_HTTP_MAX_ATTEMPTS",
        description=(
            "Max HTTP GET attempts per Vatican URL in the bulk crawl. "
            "Keep this low to avoid a single slow URL stalling a partition; "
            "use the refetch workflow for retries."
        ),
    )
    vatican_http_connect_timeout_s: float = Field(
        default=10.0,
        alias="VATICAN_HTTP_CONNECT_TIMEOUT_S",
    )
    vatican_http_read_timeout_s: float = Field(
        default=20.0,
        alias="VATICAN_HTTP_READ_TIMEOUT_S",
        description="Base read timeout per attempt (multiplied by attempt number).",
    )
    vatican_http_max_read_timeout_s: float = Field(
        default=60.0,
        alias="VATICAN_HTTP_MAX_READ_TIMEOUT_S",
    )
    vatican_http_retry_sleep_s: float = Field(
        default=1.5,
        alias="VATICAN_HTTP_RETRY_SLEEP_S",
    )
    vatican_http_direct_on_403: bool = Field(
        default=True,
        alias="VATICAN_HTTP_DIRECT_ON_403",
        description=(
            "If true, attempt a direct (non-VPN/proxy) fetch after a 403 for non-Vatican hosts."
        ),
    )
    vatican_http_direct_exclude_domains: str | None = Field(
        default="vatican.va",
        alias="VATICAN_HTTP_DIRECT_EXCLUDE_DOMAINS",
        description=(
            "Comma-separated domain suffixes that should NOT bypass VPN on 403 "
            "(e.g. vatican.va)."
        ),
    )


def load_settings() -> Settings:
    return Settings()
