package issues

import (
	"database/sql"
	"testing"
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1257(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestDatabaseSQLClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	_, err = conn.Exec(`CREATE TABLE test_1257 (
			  Str String,
			  NStr Nullable(String),
			  Dt DateTime,
			  NDt Nullable(DateTime)
		) Engine MergeTree() ORDER BY tuple()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec("DROP TABLE test_1257")
	})

	scope, err := conn.Begin()

	batch, err := scope.Prepare("INSERT INTO test_1257")
	require.NoError(t, err)
	_, err = batch.Exec(
		"str",
		"str",
		time.Now(),
		time.Now(),
	)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())

	var (
		col1string     string
		col2NullString sql.NullString
		col3Time       time.Time
		col4NullTime   sql.NullTime
	)

	row := conn.QueryRow("SELECT Str, NStr, Dt, NDt FROM test_1257")
	require.NoError(t, row.Err(), "error in row")
	require.NoError(t, row.Scan(&col1string, &col2NullString, &col3Time, &col4NullTime), "error in row.Scan")

	require.Equal(t, "str", col1string)
	require.Equal(t, "str", col2NullString.String)
}
