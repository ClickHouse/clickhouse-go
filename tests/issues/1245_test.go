package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test1245Native(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	ctx := context.Background()
	const ddl = "CREATE TABLE IF NOT EXISTS test_1245 (`id` Int32, `segment` Tuple(Tuple(UInt16, UInt16), Tuple(UInt16, UInt16))) Engine = Memory"
	require.NoError(t, conn.Exec(ctx, ddl))

	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1245"))
	}()

	require.NoError(t, conn.Exec(ctx, "INSERT INTO test_1245 VALUES (1, ((1,3),(8,9)))"))

	rows, err := conn.Query(ctx, "SELECT id, segment FROM test_1245")
	require.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next())
	var id int32
	var segment []any
	assert.Errorf(t, rows.Scan(&id, &segment), "cannot use interface for unnamed tuples, use slice")
}

func Test1245DatabaseSQLDriver(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestDatabaseSQLClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	const ddl = "CREATE TABLE IF NOT EXISTS test_1245 (`id` Int32, `segment` Tuple(Tuple(UInt16, UInt16), Tuple(UInt16, UInt16))) Engine = Memory"
	_, err = conn.Exec(ddl)
	require.NoError(t, err)

	defer func() {
		_, err = conn.Exec("DROP TABLE IF EXISTS test_1245")
		require.NoError(t, err)
	}()

	_, err = conn.Exec("INSERT INTO test_1245 VALUES (1, ((1,3),(8,9)))")
	require.NoError(t, err)

	rows, err := conn.Query("SELECT id, segment FROM test_1245")
	require.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next())
	var id int32
	var segment []any
	assert.Errorf(t, rows.Scan(&id, &segment), "cannot use interface for unnamed tuples, use slice")
}
