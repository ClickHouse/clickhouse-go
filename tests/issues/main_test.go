
package issues

import (
	"os"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

const testSet string = "issues"

func TestMain(m *testing.M) {
	os.Exit(clickhouse_tests.Runtime(m, testSet))
}

func GetIssuesTestEnvironment() (clickhouse_tests.ClickHouseTestEnvironment, error) {
	return clickhouse_tests.GetTestEnvironment(testSet)
}
