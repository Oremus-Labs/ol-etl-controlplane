from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import boto3
import httpx
import psycopg
from prefect import flow, task
from tenacity import retry, stop_after_attempt, wait_exponential

from ol_etl_controlplane.config import Settings, load_settings


@dataclass(frozen=True)
class ContractResult:
    name: str
    ok: bool
    detail: str


def _safe_detail(detail: str) -> str:
    return detail.replace("\n", " ").strip()


@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
def _http_check(url: str, headers: Optional[dict[str, str]] = None) -> tuple[int, str]:
    with httpx.Client(timeout=10.0, headers=headers, follow_redirects=True) as client:
        r = client.get(url)
        return r.status_code, r.text[:2000]


@task
def check_prefect_api(settings: Settings) -> ContractResult:
    url = settings.prefect_api_url.rstrip("/") + "/health"
    status, _ = _http_check(url)
    ok = status == 200
    return ContractResult("prefect_api", ok, f"GET {url} -> {status}")


@task
def check_postgres(settings: Settings) -> ContractResult:
    if settings.pg_dsn:
        dsn = settings.pg_dsn
    else:
        missing = [
            k
            for k, v in [
                ("POSTGRES_HOST", settings.postgres_host),
                ("POSTGRES_DB", settings.postgres_db),
                ("POSTGRES_USER", settings.postgres_user),
                ("POSTGRES_PASSWORD", settings.postgres_password),
            ]
            if not v
        ]
        if missing:
            return ContractResult(
                "postgres",
                False,
                f"Missing config: {', '.join(missing)} (or set PG_DSN)",
            )
        dsn = (
            f"postgresql://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )

    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                _ = cur.fetchone()
        return ContractResult("postgres", True, "select 1 ok")
    except Exception as e:  # noqa: BLE001
        return ContractResult("postgres", False, _safe_detail(str(e)))


@task
def check_qdrant(settings: Settings) -> ContractResult:
    url = settings.qdrant_url.rstrip("/") + "/collections"
    headers: dict[str, str] = {}
    if settings.qdrant_api_key:
        headers["api-key"] = settings.qdrant_api_key
    try:
        status, body = _http_check(url, headers=headers)
        ok = status == 200
        detail = f"GET {url} -> {status}"
        if not ok:
            detail += f" body={_safe_detail(body)[:200]}"
        return ContractResult("qdrant", ok, detail)
    except Exception as e:  # noqa: BLE001
        return ContractResult("qdrant", False, _safe_detail(str(e)))


@task
def check_embedding(settings: Settings) -> ContractResult:
    url = settings.embedding_base_url.rstrip("/")
    headers: dict[str, str] = {}
    if settings.embedding_api_key:
        headers["authorization"] = f"Bearer {settings.embedding_api_key}"
    try:
        status, _ = _http_check(url, headers=headers)
        ok = 200 <= status < 500
        return ContractResult("embedding", ok, f"GET {url} -> {status}")
    except Exception as e:  # noqa: BLE001
        return ContractResult("embedding", False, _safe_detail(str(e)))


@task
def check_s3_datasets(settings: Settings) -> ContractResult:
    missing = [k for k, v in [("S3_ACCESS_KEY", settings.s3_access_key), ("S3_SECRET_KEY", settings.s3_secret_key)] if not v]
    if missing:
        return ContractResult("s3", False, f"Missing config: {', '.join(missing)}")

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
    )

    expected = [
        f"datasets/vatican_sqlite/{settings.dataset_version}/vatican.db",
        f"datasets/newadvent_zip/{settings.dataset_version}/newadvent.zip",
    ]

    try:
        for key in expected:
            s3.head_object(Bucket=settings.s3_bucket, Key=key)
        return ContractResult("s3", True, f"head_object ok for {len(expected)} keys")
    except Exception as e:  # noqa: BLE001
        return ContractResult("s3", False, _safe_detail(str(e)))


@task
def check_nextcloud_webdav(settings: Settings) -> ContractResult:
    if not settings.nextcloud_app_password:
        return ContractResult("nextcloud", False, "Missing config: NEXTCLOUD_APP_PASSWORD")

    base = settings.nextcloud_webdav_url
    if not base.endswith("/"):
        base += "/"
    path = settings.nextcloud_ingest_path.strip("/")
    url = f"{base}{path}/"

    headers = {"Depth": "0"}
    body = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/></d:prop></d:propfind>
""".encode("utf-8")

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            r = client.request(
                "PROPFIND",
                url,
                headers=headers,
                content=body,
                auth=(settings.nextcloud_user, settings.nextcloud_app_password),
            )
        ok = r.status_code == 207
        return ContractResult("nextcloud", ok, f"PROPFIND {url} -> {r.status_code}")
    except Exception as e:  # noqa: BLE001
        return ContractResult("nextcloud", False, _safe_detail(str(e)))


@task
def check_nats(settings: Settings) -> ContractResult:
    parsed = urlparse(settings.nats_url)
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        return ContractResult("nats", False, f"Invalid NATS_URL: {settings.nats_url}")

    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            data = sock.recv(512).decode("utf-8", errors="replace")
        ok = data.startswith("INFO ")
        detail = f"tcp connect ok ({host}:{port})"
        if not ok:
            detail += f" first_bytes={json.dumps(data[:80])}"
        return ContractResult("nats", ok, detail)
    except Exception as e:  # noqa: BLE001
        return ContractResult("nats", False, _safe_detail(str(e)))


@flow(name="contracts_flow")
def contracts_flow() -> list[ContractResult]:
    settings = load_settings()

    results = [
        check_prefect_api(settings),
        check_postgres(settings),
        check_qdrant(settings),
        check_embedding(settings),
        check_s3_datasets(settings),
        check_nextcloud_webdav(settings),
        check_nats(settings),
    ]

    failed = [r for r in results if not r.ok]
    if failed:
        details = "; ".join([f"{r.name}={r.detail}" for r in failed])
        raise RuntimeError(f"Contracts failed: {details}")
    return results


if __name__ == "__main__":
    contracts_flow()

