# Prefect Terraform (ol-etl-controlplane)

Rules:
- Manage Prefect objects via Terraform only (no persistent manual UI/CLI edits).
- Point Terraform at a Prefect API endpoint **not behind Traefik basic auth**.
  - Recommended: port-forward `svc/prefect-server` and use `http://127.0.0.1:4200/api`.

## Quick start

1) Port-forward Prefect:

```bash
kubectl -n prefect port-forward svc/prefect-server 4200:4200
```

2) Terraform:

```bash
terraform init
terraform apply
```

