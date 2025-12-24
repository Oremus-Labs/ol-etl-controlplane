resource "prefect_deployment" "contracts" {
  name    = "contracts"
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

  entrypoint = "ol_etl_controlplane.flows.router_smoke_flow.router_smoke_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "nextcloud_sync" {
  name    = "nextcloud-sync"
  flow_id = prefect_flow.nextcloud_sync.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.nextcloud_sync_flow.nextcloud_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "newadvent_web_sync" {
  name    = "newadvent-web-sync"
  flow_id = prefect_flow.newadvent_web_sync.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.newadvent_web_sync_flow.newadvent_web_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "vatican_sqlite_sync" {
  name    = "vatican-sqlite-sync"
  flow_id = prefect_flow.vatican_sqlite_sync.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.vatican_sqlite_sync_flow.vatican_sqlite_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "newadvent_zip_sync" {
  name    = "newadvent-zip-sync"
  flow_id = prefect_flow.newadvent_zip_sync.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.newadvent_zip_sync_flow.newadvent_zip_sync_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "process_document" {
  name    = "process-document"
  flow_id = prefect_flow.process_document.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.process_document_flow.process_document_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "index_document" {
  name    = "index-document"
  flow_id = prefect_flow.index_document.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.index_document_flow.index_document_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "ocr_document" {
  name    = "ocr-document"
  flow_id = prefect_flow.ocr_document.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.ocr_document_flow.ocr_document_flow"

  work_pool_name  = prefect_work_pool.ocr.name
  work_queue_name = "default"

  version = "v1"
}

resource "prefect_deployment" "eval" {
  name    = "eval-nightly"
  flow_id = prefect_flow.eval.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.eval_flow.eval_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  version = "v1"
}
