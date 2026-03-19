package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test957(t *testing.T) {
	// given
	ctx := context.Background()
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	// when the client is configured to use the test environment
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, false)
	// and the client is configured to have only 1 connection
	opts.MaxIdleConns = 2
	opts.MaxOpenConns = 1
	// and the client is configured to have a connection lifetime of 1/10 of a second
	opts.ConnMaxLifetime = time.Second / 10
	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)

	// then the client should be able to execute queries for 1 second
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rows, err := conn.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}
}
