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
