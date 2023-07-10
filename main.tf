terraform {
  required_providers {
    clickhouse = {
      source = "ClickHouse/clickhouse"
      version = "~> 0.0.2"
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

variable "service_name" {
  type = string
}

variable "service_password" {
  type = string
}

provider clickhouse {
  environment     = "production"
  organization_id = var.organization_id
  token_key       = var.token_key
  token_secret    = var.token_secret
}

resource "clickhouse_service" "service" {
  name           = var.service_name
  cloud_provider = "aws"
  region         = "us-east-2"
  tier           = "development"
  idle_scaling   = true
  password  = var.service_password

  ip_access = [
    {
        source      = "0.0.0.0/0"
        description = "Anywhere"
    }
  ]
}

output "CLICKHOUSE_HOST" {
  value = clickhouse_service.service.endpoints.0.host
}

output "SERVICE_ID" {
  value = clickhouse_service.service.id
}
