package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test828(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
			"flatten_nested":     0,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	ddl := fmt.Sprintf("CREATE TABLE `%s`.`test_828` (Col1 String, Col2 UInt8) Engine MergeTree() ORDER BY tuple()", env.Database)
	defer func() {
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`test_828`", env.Database))
	}()
	conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`test_828`", env.Database))
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO `%s`.`test_828` (`Col1`, `Col2`)", env.Database))
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		"Clicky McClickHouse",
		uint8(1),
	))
	require.NoError(t, batch.Send())
}
