terraform {
  required_version = ">= 1.6.0"

  required_providers {
    prefect = {
      source  = "prefecthq/prefect"
      version = "~> 2.26"
    }
  }
}

provider "prefect" {
  endpoint = var.prefect_api_url
}
