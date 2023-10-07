package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test1067(t *testing.T) {
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
		CREATE TABLE test_1066 (
			Col1 Date32
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1066")
	}()

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1066")
	require.NoError(t, err)
	require.NoError(t, batch.Append("1970-01-02"))
	require.NoError(t, batch.Append(time.Now()))
	require.NoError(t, batch.Send())
}
