resource "prefect_deployment_schedule" "contracts_daily" {
  deployment_id = prefect_deployment.contracts.id
  cron          = "0 3 * * *"
  timezone      = "UTC"
  active        = false
}

resource "prefect_deployment_schedule" "process_index_backfill_enqueue_15m" {
  deployment_id = prefect_deployment.process_index_backfill_enqueue.id
  cron          = "*/15 * * * *"
  timezone      = "UTC"
  active        = true
  parameters = jsonencode({
    num_partitions          = 20
    min_updated_age_minutes = 15
    batch_size              = 200
  })
}
