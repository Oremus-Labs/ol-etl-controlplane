variable "prefect_api_url" {
  type        = string
  description = "Prefect API endpoint (NOT behind Traefik basic auth). Use a port-forward: http://127.0.0.1:4200/api"
  default     = "http://127.0.0.1:4200/api"
}

