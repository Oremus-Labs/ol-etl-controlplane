resource "prefect_flow" "contracts" {
  name = "contracts_flow"
}

resource "prefect_flow" "router_smoke" {
  name = "router_smoke_flow"
}

resource "prefect_flow" "nextcloud_sync" {
  name = "nextcloud_sync_flow"
}

resource "prefect_flow" "newadvent_web_sync" {
  name = "newadvent_web_sync_flow"
}

resource "prefect_flow" "vatican_sqlite_sync" {
  name = "vatican_sqlite_sync_flow"
}

resource "prefect_flow" "vatican_sqlite_enqueue" {
  name = "vatican_sqlite_enqueue_flow"
}

resource "prefect_flow" "vatican_sqlite_refetch_batch" {
  name = "vatican_sqlite_refetch_batch_flow"
}

resource "prefect_flow" "vatican_sqlite_reconcile_missing" {
  name = "vatican_sqlite_reconcile_missing_flow"
}

resource "prefect_flow" "newadvent_zip_sync" {
  name = "newadvent_zip_sync_flow"
}

resource "prefect_flow" "process_document" {
  name = "process_document_flow"
}

resource "prefect_flow" "index_document" {
  name = "index_document_flow"
}

resource "prefect_flow" "ocr_document" {
  name = "ocr_document_flow"
}

resource "prefect_flow" "eval" {
  name = "eval_flow"
}

resource "prefect_flow" "enrich_vectors" {
  name = "enrich_vectors_flow"
}

resource "prefect_flow" "quality_audit" {
  name = "quality_audit_flow"
}

resource "prefect_flow" "reconcile_index" {
  name = "reconcile_index_flow"
}

resource "prefect_flow" "metadata_backfill" {
  name = "metadata_backfill_flow"
}
