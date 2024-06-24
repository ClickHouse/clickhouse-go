package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1169(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1169 (Col1 DateTime) Engine MergeTree() ORDER BY tuple()"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1169")
	}()

	location := time.FixedZone("Etc/GMT+2", -int(time.Hour*2/time.Second))
	date, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-01-03 11:22:33", location)
	require.NoError(t, err)

	err = conn.Exec(ctx, "INSERT INTO test_1169 (Col1) VALUES (?)", date)
	require.NoError(t, err)

	// select
	var actualDate time.Time
	err = conn.QueryRow(ctx, "SELECT Col1 FROM test_1169").Scan(&actualDate)
	require.NoError(t, err)
	require.Equal(t, actualDate.In(location), date)
}
