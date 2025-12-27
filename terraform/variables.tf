variable "prefect_api_url" {
  type        = string
  description = "Prefect API endpoint (NOT behind Traefik basic auth). Default uses the external route; override via TF_VAR_prefect_api_url if needed."
  default     = "https://prefect.oremuslabs.app/api"
}
