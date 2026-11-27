package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestHTTPSummaryProgress(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_http_summary"))
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE test_http_summary (n UInt64) ENGINE = MergeTree ORDER BY n"))
	t.Cleanup(func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_http_summary"))
	})

	var progress []clickhouse.Progress
	record := clickhouse.WithProgress(func(p *clickhouse.Progress) {
		progress = append(progress, *p)
	})

	require.NoError(t, conn.Exec(clickhouse.Context(ctx, record), "INSERT INTO test_http_summary SELECT number FROM numbers(7)"))
	require.Len(t, progress, 1)
	assert.Equal(t, uint64(7), progress[0].WroteRows)
	assert.NotZero(t, progress[0].WroteBytes)

	progress = nil
	queryCtx := clickhouse.Context(ctx, record, clickhouse.WithSettings(clickhouse.Settings{"wait_end_of_query": 1}))
	rows, err := conn.Query(queryCtx, "SELECT n FROM test_http_summary")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	assert.Equal(t, 7, count)
	require.Len(t, progress, 1)
	assert.Equal(t, uint64(7), progress[0].Rows)
	assert.NotZero(t, progress[0].Bytes)
	assert.Zero(t, progress[0].WroteRows)
}
