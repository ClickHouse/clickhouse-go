package issues

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1329(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, true)
	conn, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
	require.NoError(t, err)

	_, err = conn.Exec(`CREATE TABLE test_1329 (Col String) Engine MergeTree() ORDER BY tuple()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec("DROP TABLE test_1329")
	})

	scope, err := conn.Begin()

	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO `%s`.`test_1329`", testEnv.Database))
	require.NoError(t, err)
	_, err = batch.Exec(
		"str",
	)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
}
