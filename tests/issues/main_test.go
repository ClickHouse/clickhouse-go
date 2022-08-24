package issues

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"os"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	useDocker, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}
	if !useDocker {
		fmt.Printf("Using external ClickHouse for issue IT tests -  %s:%s\n",
			clickhouse_tests.GetEnv("CLICKHOUSE_PORT", "9000"),
			clickhouse_tests.GetEnv("CLICKHOUSE_HOST", "localhost"))
		env, err := clickhouse_tests.GetExternalTestEnvironment()
		if err != nil {
			panic(err)
		}
		clickhouse_tests.SetTestEnvironment("issues", env)
		os.Exit(m.Run())
	}
	testEnv, err := clickhouse_tests.CreateClickHouseTestEnvironment("issues")
	if err != nil {
		panic(err)
	}
	defer testEnv.Container.Terminate(context.Background()) //nolint
	os.Exit(m.Run())
}
