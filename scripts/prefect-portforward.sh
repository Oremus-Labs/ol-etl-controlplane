#!/usr/bin/env bash
set -euo pipefail

kubectl -n prefect port-forward svc/prefect-server 4200:4200

