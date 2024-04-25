terraform {
  required_providers {
    clickhouse = {
      source  = "ClickHouse/clickhouse"
      version = "0.0.6"
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
  default = "clickhouse-go-tests"
}

variable "service_password" {
  type = string
}

variable "api_url" {
  type = string
}

variable "allowed_cidr" {
  type = string
  default = "0.0.0.0/0"
}

provider "clickhouse" {
  organization_id = var.organization_id
  token_key       = var.token_key
  token_secret    = var.token_secret
  api_url         = var.api_url
}

resource "clickhouse_service" "service" {
  name           = var.service_name
  cloud_provider = "aws"
  region         = "us-east-2"
  tier           = "development"
  password       = var.service_password

  ip_access = [
    {
      source      = var.allowed_cidr
      description = "Allowed CIDR"
    }
  ]
}

output "CLICKHOUSE_HOST" {
  value = clickhouse_service.service.endpoints.0.host
}

output "SERVICE_ID" {
  value = clickhouse_service.service.id
}
