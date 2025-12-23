from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_version: str = Field(alias="PIPELINE_VERSION")
    dataset_version: str = Field(alias="DATASET_VERSION")

    prefect_api_url: str = Field(alias="PREFECT_API_URL")

    pg_dsn: Optional[str] = Field(default=None, alias="PG_DSN")
    postgres_host: Optional[str] = Field(default=None, alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: Optional[str] = Field(default=None, alias="POSTGRES_DB")
    postgres_user: Optional[str] = Field(default=None, alias="POSTGRES_USER")
    postgres_password: Optional[str] = Field(default=None, alias="POSTGRES_PASSWORD")

    qdrant_url: str = Field(alias="QDRANT_URL")
    qdrant_api_key: Optional[str] = Field(default=None, alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(default="library_chunks_v1", alias="QDRANT_COLLECTION")

    embedding_base_url: str = Field(alias="EMBEDDING_BASE_URL")
    embedding_api_key: Optional[str] = Field(default=None, alias="EMBEDDING_API_KEY")

    s3_endpoint: str = Field(alias="S3_ENDPOINT")
    s3_bucket: str = Field(alias="S3_BUCKET")
    s3_access_key: Optional[str] = Field(default=None, alias="S3_ACCESS_KEY")
    s3_secret_key: Optional[str] = Field(default=None, alias="S3_SECRET_KEY")

    nextcloud_webdav_url: str = Field(alias="NEXTCLOUD_WEBDAV_URL")
    nextcloud_user: str = Field(alias="NEXTCLOUD_USER")
    nextcloud_app_password: Optional[str] = Field(default=None, alias="NEXTCLOUD_APP_PASSWORD")
    nextcloud_ingest_path: str = Field(default="ETL/Ingest", alias="NEXTCLOUD_INGEST_PATH")

    nats_url: str = Field(alias="NATS_URL")


def load_settings() -> Settings:
    return Settings()

