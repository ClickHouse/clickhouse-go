package issues

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// https://github.com/ClickHouse/clickhouse-go/issues/2016
func Test2016(t *testing.T) {
	te, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := clickhouse_tests.ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	conn, err := clickhouse_tests.GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	clickhouse_tests.CleanupNativeConn(t, conn)

	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS target_stream_error (x UInt64) ENGINE = MergeTree ORDER BY tuple()"))
	t.Cleanup(func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS target_stream_error"))
	})

	// Small single-threaded blocks guarantee the server streams several data
	// blocks before throwIf fires, so the failure happens mid-stream.
	srcCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 1000,
		"max_threads":    1,
	}))
	sourceRows, err := conn.Query(srcCtx, "SELECT number AS x FROM system.numbers WHERE throwIf(number = 50000, 'mid-stream failure') = 0 LIMIT 100000")
	require.NoError(t, err)
	defer sourceRows.Close()

	b, err := conn.PrepareBatch(ctx, "INSERT INTO target_stream_error")
	require.NoError(t, err)

	appendErr := b.Append(sourceRows)
	require.Error(t, appendErr)
	var appendException *clickhouse.Exception
	require.True(t, errors.As(appendErr, &appendException))
	assert.Equal(t, int32(395), appendException.Code)

	sendErr := b.Send()
	require.ErrorIs(t, sendErr, clickhouse.ErrBatchInvalid)
	var sendException *clickhouse.Exception
	require.True(t, errors.As(sendErr, &sendException))
	assert.Equal(t, int32(395), sendException.Code)
}
