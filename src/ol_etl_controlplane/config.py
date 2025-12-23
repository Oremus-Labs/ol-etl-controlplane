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

    embedding_base_url: str = Field(alias="EMBEDDING_BASE_URL")
    embedding_api_key: str | None = Field(default=None, alias="EMBEDDING_API_KEY")

    s3_endpoint: str = Field(alias="S3_ENDPOINT")
    s3_bucket: str = Field(alias="S3_BUCKET")
    s3_access_key: str | None = Field(default=None, alias="S3_ACCESS_KEY")
    s3_secret_key: str | None = Field(default=None, alias="S3_SECRET_KEY")

    nextcloud_webdav_url: str = Field(alias="NEXTCLOUD_WEBDAV_URL")
    nextcloud_user: str = Field(alias="NEXTCLOUD_USER")
    nextcloud_app_password: str | None = Field(default=None, alias="NEXTCLOUD_APP_PASSWORD")
    nextcloud_ingest_path: str = Field(default="ETL/Ingest", alias="NEXTCLOUD_INGEST_PATH")

    nats_url: str = Field(alias="NATS_URL")
    nats_subject: str = Field(default="docs.discovered", alias="NATS_SUBJECT")

    nextcloud_max_files: int = Field(default=25, alias="NEXTCLOUD_MAX_FILES")
    newadvent_seed_urls: str | None = Field(default=None, alias="NEWADVENT_SEED_URLS")
    newadvent_web_max_pages: int = Field(default=25, alias="NEWADVENT_WEB_MAX_PAGES")
    vatican_sqlite_max_rows: int = Field(default=25, alias="VATICAN_SQLITE_MAX_ROWS")
    newadvent_zip_max_entries: int = Field(default=50, alias="NEWADVENT_ZIP_MAX_ENTRIES")


def load_settings() -> Settings:
    return Settings()
