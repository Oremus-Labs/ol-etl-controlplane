ARG PREFECT_IMAGE=prefecthq/prefect:3.6.7-python3.12
FROM ${PREFECT_IMAGE}

ARG OL_RAG_PIPELINE_CORE_REF=main

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install pipeline-core from a pinned ref (tarball URL avoids requiring git in the image).
RUN python -m pip install --upgrade pip && \
    python -m pip install "https://github.com/Oremus-Labs/ol-rag-pipeline-core/archive/${OL_RAG_PIPELINE_CORE_REF}.tar.gz"

# Install controlplane runtime dependencies (prefect is already in the base image).
RUN python -m pip install \
      "boto3>=1.34.0" \
      "httpx>=0.27.0" \
      "psycopg[binary]>=3.2.0" \
      "pydantic>=2.7.0" \
      "pydantic-settings>=2.3.0" \
      "tenacity>=8.3.0"

WORKDIR /app
COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src

# Install this package without re-resolving deps (pipeline-core is already installed above).
RUN python -m pip install --no-deps .

