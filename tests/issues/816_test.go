package issues

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func Test816(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	const ddl = `
		CREATE TABLE test_816 (
			  Col1 Tuple(count Nullable(Int64), products Array(Tuple(price Nullable(Float64), qty Nullable(Int64))), price Nullable(Float64))
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec("DROP TABLE test_816")
	defer func() {
		conn.Exec("DROP TABLE test_816")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO test_816")
	require.NoError(t, err)
	var (
		col1Data = map[string]interface{}{
			"count": nil,
			"products": []map[string]interface{}{
				{
					"price": nil,
					"qty":   nil,
				},
				{
					"price": float64(2.3),
					"qty":   int64(2),
				},
			},
			"price": float64(1.1),
		}
	)
	_, err = batch.Exec(col1Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var col1 interface{}
	require.NoError(t, conn.QueryRow("SELECT Col1 FROM test_816").Scan(&col1))
	assert.Equal(t, clickhouse_std_tests.ToJson(col1Data), clickhouse_std_tests.ToJson(col1))
}
