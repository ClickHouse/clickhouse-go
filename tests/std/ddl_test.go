
package std

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestQuotedDDL(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	ctx := context.Background()
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			require.NoError(t, conn.PingContext(context.Background()))
			require.NoError(t, err)
			require.NoError(t, conn.Ping())
			conn.Exec("DROP TABLE `std_test_ddl`")
			defer func() {
				conn.Exec("DROP TABLE `std_test_ddl`")
			}()
			_, err = conn.Exec("CREATE TABLE `std_test_ddl` (`1` String) Engine MergeTree() ORDER BY tuple()")
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.PrepareContext(ctx, "INSERT INTO `std_test_ddl`")
			require.NoError(t, err)
			_, err = batch.Exec("A")
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
		})
	}
}
