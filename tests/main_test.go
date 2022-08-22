package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	useDocker := strings.ToLower(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if useDocker == "false" {
		fmt.Printf("Using external ClickHouse for IT tests -  %s:%s\n",
			GetEnv("CLICKHOUSE_PORT", "9000"),
			GetEnv("CLICKHOUSE_HOST", "localhost"))
		os.Exit(m.Run())
	}
	testEnv, err := GetClickHouseTestEnvironment("native")
	if err != nil {
		panic(err)
	}
	defer testEnv.container.Terminate(context.Background()) //nolint
	os.Exit(m.Run())
}
