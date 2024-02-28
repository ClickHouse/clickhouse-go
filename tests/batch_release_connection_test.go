package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

func TestBatchReleaseConnection(t *testing.T) {
	SkipOnCloud(t, "This test is flaky on cloud ClickHouse")

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)

	const tableName = "test_release_connection"

	var ddl = fmt.Sprintf(`
		CREATE TABLE %s (
			  Col1 UInt64
			, Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`, tableName)
	defer func() {
		dropTable(conn, tableName)
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName), driver.WithReleaseConnection())
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint64(1), "test"))
	require.NoError(t, batch.Send())
	require.Equal(t, uint64(1), getRowsCount(t, conn, tableName))

	require.NoError(t, batch.Send())
	require.Equal(t, uint64(2), getRowsCount(t, conn, tableName))

	deduplicateTable(t, conn, tableName)
	require.Equal(t, uint64(1), getRowsCount(t, conn, tableName))
}

func TestBatchReleaseConnectionFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)

	const tableName = "test_release_connection_flush"

	var ddl = fmt.Sprintf(`
		CREATE TABLE %s (
			  Col1 UInt64
			, Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`, tableName)
	defer func() {
		dropTable(conn, tableName)
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName), driver.WithReleaseConnection())
	require.NoError(t, err)

	require.NoError(t, batch.Append(uint64(1), "test"))
	require.NoError(t, batch.Flush())

	require.NoError(t, batch.Send())

	require.Equal(t, uint64(1), getRowsCount(t, conn, tableName))
}
