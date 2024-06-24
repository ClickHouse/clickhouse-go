package issues

import (
	"context"
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1106(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1106 (col1 IPv6, col2 IPv6, col3 IPv4, col4 IPv4) Engine MergeTree() ORDER BY tuple()"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1106")
	}()

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1106")
	require.NoError(t, err)

	var ip net.IP
	var ipPtr *net.IP

	require.NoError(t, batch.Append(ip, ipPtr, ip, ipPtr))
	require.NoError(t, batch.Send())
}
