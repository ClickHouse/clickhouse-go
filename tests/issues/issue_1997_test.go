package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1997_NativeInsertSingleQueryID is a regression test for
// https://github.com/ClickHouse/clickhouse-go/issues/1997: a single native
// PrepareBatch/Send used to produce two `system.query_log` rows with distinct
// `query_id` values on ClickHouse 26.3+, because the server defaulted
// `async_insert=1` and flushed the buffer as a rewritten
// `INSERT INTO db.table` under a new UUID. PrepareBatch already buffers
// client-side, so the driver now sends `async_insert=0` unless the caller set it.
func TestIssue1997_NativeInsertSingleQueryID(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionTCPWithOptions("issues", nil, nil, nil, func(o *clickhouse.Options) {
		// Drop the suite default so this test sees the server default
		// (async_insert=1 on 26.3+), which is what triggered the duplicate log.
		delete(o.Settings, "async_insert")
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()
	table := fmt.Sprintf("issue_1997_%s", uuid.NewString()[:8])
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id UInt64, value String) ENGINE = MergeTree() ORDER BY id`, table)))
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Logf("DROP TABLE %s failed: %v", table, err)
		}
	})

	queryID := "issue-1997-" + uuid.NewString()
	insertCtx := clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))

	batch, err := conn.PrepareBatch(insertCtx, fmt.Sprintf("INSERT INTO %s (id, value)", table))
	require.NoError(t, err)
	defer batch.Close()

	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(uint64(i), fmt.Sprintf("row-%d", i)))
	}
	require.NoError(t, batch.Send())

	var stored uint64
	require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&stored))
	require.Equal(t, uint64(10), stored)

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Skipf("`system.query_log` not available: %v", err)
	}

	logFilter := fmt.Sprintf(`
		type = 'QueryFinish'
		AND current_database = currentDatabase()
		AND startsWith(upper(trimLeft(query)), 'INSERT')
		AND query ILIKE '%%%s%%'
	`, table)

	var distinctIDs uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT uniqExact(query_id) FROM system.query_log WHERE "+logFilter).Scan(&distinctIDs))
	require.Equal(t, uint64(1), distinctIDs, "native INSERT must log a single query_id, got %d", distinctIDs)

	var loggedID string
	require.NoError(t, conn.QueryRow(ctx, "SELECT any(query_id) FROM system.query_log WHERE "+logFilter).Scan(&loggedID))
	require.Equal(t, queryID, loggedID)
}
