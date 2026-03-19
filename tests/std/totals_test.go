package std

import (
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestStdWithTotals(t *testing.T) {
	const query = `
	SELECT
		number AS n
		, COUNT()
	FROM (
		SELECT number FROM system.numbers LIMIT 100
	) GROUP BY n WITH TOTALS
	`
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := GetStdDSNConnection(clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	rows, err := conn.Query(query)
	require.NoError(t, err)
	var count int
	for rows.Next() {
		count++
		var (
			n uint64
			c uint64
		)
		require.NoError(t, rows.Scan(&n, &c))
	}
	require.Equal(t, 100, count)
	require.True(t, rows.NextResultSet())
	count = 0
	for rows.Next() {
		count++
		var (
			n, totals uint64
		)
		require.NoError(t, rows.Scan(&n, &totals))
		assert.Equal(t, uint64(0), n)
		assert.Equal(t, uint64(100), totals)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count)
}
