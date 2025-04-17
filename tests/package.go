package tests

import (
	"os"
	"strings"
	"testing"
)

var LocalClickHouse = false
var RemoteClickHouse = false
var CloudClickHouse = false

func init() {
	if host, found := os.LookupEnv("CLICKHOUSE_HOST"); found {
		if strings.HasSuffix(host, "clickhouse.cloud") ||
			strings.HasSuffix(host, "clickhouse-staging.com") {
			CloudClickHouse = true
		} else {
			RemoteClickHouse = true
		}
	} else {
		LocalClickHouse = true
	}
}

// SkipOnCloud skips the test if it's run on ClickHouse Cloud
func SkipOnCloud(t *testing.T, reasons ...string) {
	if CloudClickHouse {
		t.Skip(append(
			[]string{"Skipping test on cloud ClickHouse"},
			reasons...,
		))
	}
}

// SkipNotCloud skips the test if it's not run on ClickHouse Cloud
func SkipNotCloud(t *testing.T, reasons ...string) {
	if !CloudClickHouse {
		t.Skip(append(
			[]string{"Skipping test for non-cloud ClickHouse"},
			reasons...,
		))
	}
}
