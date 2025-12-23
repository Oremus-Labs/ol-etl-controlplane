from ol_etl_controlplane.config import Settings


def test_settings_parses_required_fields() -> None:
    settings = Settings.model_validate(
        {
            "PIPELINE_VERSION": "v1",
            "DATASET_VERSION": "2025-12-23",
            "PREFECT_API_URL": "http://127.0.0.1:4200/api",
            "QDRANT_URL": "http://localhost:6333",
            "EMBEDDING_BASE_URL": "http://localhost:8080",
            "S3_ENDPOINT": "http://localhost:9000",
            "S3_BUCKET": "rag-artifacts",
            "NEXTCLOUD_WEBDAV_URL": "http://localhost/remote.php/dav/files/etl-bot/",
            "NEXTCLOUD_USER": "etl-bot",
            "NATS_URL": "nats://localhost:4222",
        }
    )
    assert settings.pipeline_version == "v1"
    assert settings.dataset_version == "2025-12-23"

