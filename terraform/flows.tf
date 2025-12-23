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
