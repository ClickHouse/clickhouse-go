# ClickHouse Terraform Provider

## Local Development

To test the provider locally, you'll need to set up some dev overrides so that terraform knows where to grab your local installation.

First, find the `GOBIN` path where Go installs your binaries: 

```sh
$ go env GOBIN
/Users/<Username>/go/bin
```

If the GOBIN go environment variable is not set, use the default path, `/Users/<Username>/go/bin`.

Then, create a new file called .terraformrc in your home directory (~), then add the dev_overrides block below. Change the `<PATH>` to the value returned from the go env GOBIN command above.

```t
provider_installation {

  dev_overrides {
      "clickhouse.cloud/terraform/clickhouse" = "<PATH>"
  }

  # For all other providers, install them directly from their origin provider
  # registries as normal. If you omit this, Terraform will _only_ use
  # the dev_overrides block, and so no other providers will be available.
  direct {}
}
```

Next, install the provider to the `GOBIN` path:

```sh
$ go install .
```

Finally, running `terraform plan` or `terraform apply` while in the examples/basic directory will showcase a basic usage of the plugin (the dev_overrides make it so that you have to skip `terraform init`):

```
terraform apply -var-file="variables.tfvars"
╷
│ Warning: Provider development overrides are in effect
│
│ The following provider development overrides are set in the CLI configuration:
│  - clickhouse.cloud/terraform/clickhouse in /Users/kinzeng/go/bin
│
│ The behavior may therefore not match any released version of the provider and applying changes may
│ cause the state to become incompatible with published releases.
╵

Terraform used the selected providers to generate the following execution plan. Resource actions are
indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # clickhouse_service.service will be created
  + resource "clickhouse_service" "service" {
      + cloud_provider       = "aws"
      + id                   = (known after apply)
      + idle_scaling         = true
      + idle_timeout_minutes = 5
      + ip_access            = [
          + {
              + description = "Test IP"
              + source      = "192.168.2.63"
            },
        ]
      + last_updated         = (known after apply)
      + max_total_memory_gb  = 360
      + min_total_memory_gb  = 24
      + name                 = "My Service"
      + region               = "us-east-1"
      + tier                 = "production"
    }

Plan: 1 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Note that this basic example points to a locally running OpenAPI server. If you do not have that server running, you can still test by changing the `environment` key on the provider to `qa`, `staging`, or `production`. Make sure to change the organization id, token key, and token secret to valid values for those environments.
