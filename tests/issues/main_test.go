package issues

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	useDocker := strings.ToLower(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if useDocker == "false" {
		fmt.Printf("Using external ClickHouse for IT tests -  %s:%s\n",
			clickhouse_tests.GetEnv("CLICKHOUSE_PORT", "9000"),
			clickhouse_tests.GetEnv("CLICKHOUSE_HOST", "localhost"))
		// TODO: Set environment
		os.Exit(m.Run())
	}
	testEnv, err := clickhouse_tests.CreateClickHouseTestEnvironment("issues")
	if err != nil {
		panic(err)
	}
	defer testEnv.Container.Terminate(context.Background()) //nolint
	os.Exit(m.Run())
}
