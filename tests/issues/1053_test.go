package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test1053(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_1053 (
			Col1 UInt64
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1053")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO test_1053`)
	require.NoError(t, err)

	column := batch.Column(1000) // doesn't exist column

	require.Error(t, column.Append(uint64(1)))
	require.Error(t, column.AppendRow(uint64(1)))
}
