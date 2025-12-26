# ol-etl-controlplane

Prefect control plane for the OL Enterprise ELT + RAG platform.

Source of truth:
- Prefect objects (work pools/deployments/schedules): `terraform/`
- Flows (Python): `src/ol_etl_controlplane/flows/`

See `../PLAN.md` for architecture + phase plan.

## Prefect worker image (recommended)

Kubernetes worker pods previously did `pip install git+...` at container start to get this repo + `ol-rag-pipeline-core`,
which slows rollouts and can fail due to network/transient issues.

This repo includes a Dockerfile + CI workflow to publish a prebuilt worker image to GHCR:
- Dockerfile: `docker/prefect-worker/Dockerfile`
- CI: `.github/workflows/build-prefect-worker-image.yml`
- Image: `ghcr.io/oremus-labs/prefect-worker`
  - Rolling tag: `prefect-3.6.7-py3.12-main`
  - Immutable tag: `prefect-3.6.7-py3.12-<git-sha>`

Once an image is published, switch GitOps (`ol-kubernetes-cluster`) to use it and disable bootstrap installs.
