terraform {
  required_providers {
    clickhouse = {
      source = "ClickHouse/clickhouse"
      version = "0.0.2"
    }
  }
}

variable "organization_id" {
  type = string
}

variable "token_key" {
  type = string
}

variable "token_secret" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "cluster_password" {
  type = string
}

provider clickhouse {
  environment     = "production"
  organization_id = var.organization_id
  token_key       = var.token_key
  token_secret    = var.token_secret
}

resource "clickhouse_service" "service" {
  name           = var.cluster_name
  cloud_provider = "aws"
  region         = "us-east-2"
  tier           = "production"
  idle_scaling   = true
  password  = var.cluster_password

  ip_access = [
    {
        source      = "0.0.0.0/0"
        description = "Anywhere"
    }
  ]

  min_total_memory_gb  = 24
  max_total_memory_gb  = 360
  idle_timeout_minutes = 30
}

output "CLICKHOUSE_HOST" {
  value = clickhouse_service.service.endpoints.0.host
}
