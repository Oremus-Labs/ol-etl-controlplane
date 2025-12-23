resource "prefect_deployment_schedule" "contracts_daily" {
  deployment_id = prefect_deployment.contracts.id
  cron          = "0 3 * * *"
  timezone      = "UTC"
  active        = false
}

