package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

func TestDriverCollectRows(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_driver_collect_rows (
			  Col1 UInt64
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_driver_collect_rows")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_driver_collect_rows")
	require.NoError(t, err)
	var (
		testData = []uint64{1, 2, 3, 4, 5}
	)
	for _, testInt := range testData {
		require.NoError(t, batch.Append(testInt))
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_driver_collect_rows")
	require.NoError(t, err)
	gotData, err := driver.CollectRows(
		rows,
		func(row driver.CollectableRow) (uint64, error) {
			var i uint64
			err = row.Scan(&i)
			return i, err
		},
	)
	require.NoError(t, err)
	require.Equal(t, testData, gotData)
}

func TestDriverAppendRows(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_driver_append_rows (
			  Col1 UInt64
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_driver_append_rows")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_driver_append_rows")
	require.NoError(t, err)
	var (
		testData = []uint64{1, 2, 3, 4, 5}
	)
	for _, testInt := range testData {
		require.NoError(t, batch.Append(testInt))
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_driver_append_rows")
	require.NoError(t, err)
	const rowsSliceCap = 10
	gotData, err := driver.AppendRows(
		make([]uint64, 0, rowsSliceCap),
		rows,
		func(row driver.CollectableRow) (uint64, error) {
			var i uint64
			err = row.Scan(&i)
			return i, err
		},
	)
	require.NoError(t, err)
	require.Equal(t, testData, gotData)
	require.Equal(t, rowsSliceCap, cap(gotData))
}
