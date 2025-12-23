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

locals {
  controlplane_pull_steps = [
    {
      type       = "git_clone"
      repository = "https://github.com/Oremus-Labs/ol-etl-controlplane.git"
      branch     = "main"
    },
    {
      type      = "pip_install_requirements"
      directory = "ol-etl-controlplane"
    },
  ]
}

resource "prefect_deployment" "nextcloud_sync" {
  name    = "nextcloud-sync"
  flow_id = prefect_flow.nextcloud_sync.id

  paused = false

  pull_steps = local.controlplane_pull_steps
  path       = "ol-etl-controlplane"
  entrypoint = "src/ol_etl_controlplane/flows/nextcloud_sync_flow.py:nextcloud_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "newadvent_web_sync" {
  name    = "newadvent-web-sync"
  flow_id = prefect_flow.newadvent_web_sync.id

  paused = false

  pull_steps = local.controlplane_pull_steps
  path       = "ol-etl-controlplane"
  entrypoint = "src/ol_etl_controlplane/flows/newadvent_web_sync_flow.py:newadvent_web_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "vatican_sqlite_sync" {
  name    = "vatican-sqlite-sync"
  flow_id = prefect_flow.vatican_sqlite_sync.id

  paused = false

  pull_steps = local.controlplane_pull_steps
  path       = "ol-etl-controlplane"
  entrypoint = "src/ol_etl_controlplane/flows/vatican_sqlite_sync_flow.py:vatican_sqlite_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "newadvent_zip_sync" {
  name    = "newadvent-zip-sync"
  flow_id = prefect_flow.newadvent_zip_sync.id

  paused = false

  pull_steps = local.controlplane_pull_steps
  path       = "ol-etl-controlplane"
  entrypoint = "src/ol_etl_controlplane/flows/newadvent_zip_sync_flow.py:newadvent_zip_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}
