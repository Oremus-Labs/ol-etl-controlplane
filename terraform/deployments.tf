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

  # Crawl pool has VPN egress and is tuned for long-running external fetches.
  work_pool_name  = prefect_work_pool.crawl.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      partition_index  = { type = "integer" }
      num_partitions   = { type = "integer" }
      allow_single_run = { type = "boolean" }
      max_rows         = { type = "integer" }
    }
    additionalProperties = true
  })

  version = "v2"
}

resource "prefect_deployment" "vatican_sqlite_enqueue" {
  name    = "vatican-sqlite-enqueue"
  flow_id = prefect_flow.vatican_sqlite_enqueue.id

  # Day-1 bulk ingest tool: run manually when you want to backfill the Vatican dataset.
  paused = false

  entrypoint = "ol_etl_controlplane.flows.vatican_sqlite_enqueue_flow.vatican_sqlite_enqueue_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      num_partitions  = { type = "integer" }
      start_partition = { type = "integer" }
      end_partition   = { type = "integer" }
      deployment_fqn  = { type = "string" }
      max_rows        = { type = "integer" }
    }
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "vatican_sqlite_refetch_batch" {
  name    = "vatican-sqlite-refetch-batch"
  flow_id = prefect_flow.vatican_sqlite_refetch_batch.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.vatican_sqlite_refetch_batch_flow.vatican_sqlite_refetch_batch_flow"

  # Needs VPN egress (external fetch).
  work_pool_name  = prefect_work_pool.crawl.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      urls           = { type = "array", items = { type = "string" } }
      publish_events = { type = "boolean" }
      force          = { type = "boolean" }
    }
    required             = ["urls"]
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "vatican_sqlite_reconcile_missing" {
  name    = "vatican-sqlite-reconcile-missing"
  flow_id = prefect_flow.vatican_sqlite_reconcile_missing.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.vatican_sqlite_reconcile_missing_flow.vatican_sqlite_reconcile_missing_flow"

  # This only computes gaps and enqueues refetch batches (no external fetch).
  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      batch_size             = { type = "integer" }
      max_urls               = { type = "integer" }
      include_missing_raw    = { type = "boolean" }
      publish_events         = { type = "boolean" }
      refetch_deployment_fqn = { type = "string" }
    }
    additionalProperties = true
  })

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

  # Prefect Terraform provider cannot introspect Python signatures to build parameter schemas.
  # Define a minimal schema so CLI/API callers can pass parameters without Prefect rejecting them.
  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      document_id         = { type = "string" }
      pipeline_version    = { type = "string" }
      content_fingerprint = { type = "string" }
      source              = { type = "string" }
      event_id            = { type = "string" }
      event               = { type = "object" }
      force               = { type = "boolean" }
    }
    required             = ["document_id"]
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "index_document" {
  name    = "index-document"
  flow_id = prefect_flow.index_document.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.index_document_flow.index_document_flow"

  # Indexing is heavy (embeddings + Qdrant writes). Run it in a dedicated pool to
  # keep pool-general responsive and to cap concurrent embedding load.
  work_pool_name  = prefect_work_pool.index.name
  work_queue_name = "default"

  # See note above.
  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      document_id         = { type = "string" }
      pipeline_version    = { type = "string" }
      content_fingerprint = { type = "string" }
      source              = { type = "string" }
      event_id            = { type = "string" }
      event               = { type = "object" }
    }
    required             = ["document_id"]
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "ocr_document" {
  name    = "ocr-document"
  flow_id = prefect_flow.ocr_document.id

  paused = false

  entrypoint = "ol_etl_controlplane.flows.ocr_document_flow.ocr_document_flow"

  work_pool_name  = prefect_work_pool.ocr.name
  work_queue_name = "default"

  # See note above.
  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      document_id         = { type = "string" }
      pipeline_version    = { type = "string" }
      content_fingerprint = { type = "string" }
      source              = { type = "string" }
      event_id            = { type = "string" }
      event               = { type = "object" }
      force               = { type = "boolean" }
    }
    required             = ["document_id"]
    additionalProperties = true
  })

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

resource "prefect_deployment" "enrich_vectors" {
  name    = "enrich-vectors"
  flow_id = prefect_flow.enrich_vectors.id

  # Day-2 job: run manually or via an explicit schedule later.
  paused = true

  entrypoint = "ol_etl_controlplane.flows.enrich_vectors_flow.enrich_vectors_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      pipeline_version     = { type = "string" }
      enrichment_version   = { type = "string" }
      model                = { type = "string" }
      confidence_threshold = { type = "number" }
      source               = { type = "string" }
      limit_chunks         = { type = "integer" }
      include_rejected     = { type = "boolean" }
      dry_run              = { type = "boolean" }
    }
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "quality_audit" {
  name    = "quality-audit"
  flow_id = prefect_flow.quality_audit.id

  paused = true

  entrypoint = "ol_etl_controlplane.flows.quality_audit_flow.quality_audit_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      pipeline_version     = { type = "string" }
      qdrant_collection    = { type = "string" }
      sources_csv          = { type = "string" }
      max_docs_per_source  = { type = "integer" }
      min_extract_chars    = { type = "integer" }
      max_html_tag_ratio   = { type = "number" }
      save_report_to_minio = { type = "boolean" }
    }
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "reconcile_index" {
  name    = "reconcile-index"
  flow_id = prefect_flow.reconcile_index.id

  paused = true

  entrypoint = "ol_etl_controlplane.flows.reconcile_index_flow.reconcile_index_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      pipeline_version          = { type = "string" }
      qdrant_collection         = { type = "string" }
      max_docs                  = { type = "integer" }
      sources_csv               = { type = "string" }
      dry_run                   = { type = "boolean" }
      delete_dotfiles           = { type = "boolean" }
      delete_test_docs          = { type = "boolean" }
      delete_meta_refresh_stubs = { type = "boolean" }
    }
    additionalProperties = true
  })

  version = "v1"
}

resource "prefect_deployment" "metadata_backfill" {
  name    = "metadata-backfill"
  flow_id = prefect_flow.metadata_backfill.id

  # Day-2 operator tool.
  paused = true

  entrypoint = "ol_etl_controlplane.flows.metadata_backfill_flow.metadata_backfill_flow"

  work_pool_name  = prefect_work_pool.general.name
  work_queue_name = "default"

  enforce_parameter_schema = false
  parameter_openapi_schema = jsonencode({
    type = "object"
    properties = {
      max_docs    = { type = "integer" }
      sources_csv = { type = "string" }
      dry_run     = { type = "boolean" }
    }
    additionalProperties = true
  })

  version = "v1"
}
