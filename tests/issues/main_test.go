package issues

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const testSet string = "issues"

func TestMain(m *testing.M) {
	seed := time.Now().UnixNano()
	fmt.Printf("using random seed %d for %s\n", seed, testSet)
	rand.Seed(seed)
	useDocker, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}
	var env clickhouse_tests.ClickHouseTestEnvironment
	switch useDocker {
	case true:
		env, err = clickhouse_tests.CreateClickHouseTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
		defer env.Container.Terminate(context.Background()) //nolint
	case false:
		env, err = clickhouse_tests.GetExternalTestEnvironment(testSet)
		if err != nil {
			panic(err)
		}
	}
	clickhouse_tests.SetTestEnvironment(testSet, env)
	if err := clickhouse_tests.CreateDatabase(testSet); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func GetIssuesTestEnvironment() (clickhouse_tests.ClickHouseTestEnvironment, error) {
	return clickhouse_tests.GetTestEnvironment(testSet)
}
