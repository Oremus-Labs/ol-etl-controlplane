terraform {
  backend "s3" {
    bucket = "workspaces"
    key    = "ol-etl-controlplane/terraform.tfstate"
    region = "us-east-1"

    endpoints = {
      s3 = "https://s3.oremuslabs.app"
    }

    skip_credentials_validation = true
    skip_region_validation      = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    use_path_style              = true
    shared_credentials_files    = ["~/.aws/credentials"]
    profile                     = "default"
  }
}

