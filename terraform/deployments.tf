resource "prefect_deployment" "contracts" {
  name   = "contracts"
  flow_id = prefect_flow.contracts.id

  paused = true

  # This is a placeholder until we standardize how flow code is delivered to workers.
  # Phase 0 acceptance does not require running via deployment; it requires the flow and pools exist as IaC.
  entrypoint = "src/ol_etl_controlplane/flows/contracts_flow.py:contracts_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}
