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

resource "prefect_deployment" "router_smoke" {
  name    = "router-smoke"
  flow_id = prefect_flow.router_smoke.id

  paused = false

  pull_steps = [
    {
      type       = "git_clone"
      repository = "https://github.com/Oremus-Labs/ol-etl-controlplane.git"
      branch     = "main"
    }
  ]

  # The git_clone step clones into a directory named after the repo.
  path       = "ol-etl-controlplane"
  entrypoint = "src/ol_etl_controlplane/flows/router_smoke_flow.py:router_smoke_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}
