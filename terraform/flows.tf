resource "prefect_flow" "contracts" {
  name = "contracts_flow"
}

resource "prefect_flow" "router_smoke" {
  name = "router_smoke_flow"
}
