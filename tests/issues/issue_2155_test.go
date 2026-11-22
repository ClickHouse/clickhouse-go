package issues

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2155 verifies that batch.Send returns the error when the driver cannot encode
// the block, and that the server inserts no rows. The native protocol returns the error.
// Over HTTP, Send returned nil for all compression methods.
// See https://github.com/ClickHouse/clickhouse-go/issues/2155.
func Test2155(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	testCases := []struct {
		protocol clickhouse.Protocol
		method   clickhouse.CompressionMethod
	}{
		{clickhouse.Native, clickhouse.CompressionNone},
		{clickhouse.Native, clickhouse.CompressionLZ4},
		{clickhouse.Native, clickhouse.CompressionZSTD},
		{clickhouse.HTTP, clickhouse.CompressionNone},
		{clickhouse.HTTP, clickhouse.CompressionLZ4},
		{clickhouse.HTTP, clickhouse.CompressionZSTD},
		{clickhouse.HTTP, clickhouse.CompressionGZIP},
		{clickhouse.HTTP, clickhouse.CompressionDeflate},
		{clickhouse.HTTP, clickhouse.CompressionBrotli},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v %v", tc.protocol, tc.method), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, tc.protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, &clickhouse.Compression{
				Method: tc.method,
			})
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()

			tableName := fmt.Sprintf("test_2155_%v_%v", tc.protocol, tc.method)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple()", tableName)))
			t.Cleanup(func() {
				if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			// Column a gets 3 rows and column b gets 2, so the block does not encode.
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
			require.NoError(t, err)
			require.NoError(t, batch.Column(0).Append([]uint64{1, 2, 3}))
			require.NoError(t, batch.Column(1).Append([]string{"x", "y"}))
			err = batch.Send()
			require.Error(t, err, "Send must return the encode error")
			var blockErr *proto.BlockError
			assert.True(t, errors.As(err, &blockErr), "Send must return a *proto.BlockError, got %v", err)
			assert.ErrorContains(t, err, "mismatched len of columns")

			// The connection pool still works, and the failed batch inserted no rows.
			batch, err = conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
			require.NoError(t, err)
			require.NoError(t, batch.Append(uint64(4), "z"))
			require.NoError(t, batch.Send())
			var count uint64
			require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM "+tableName).Scan(&count))
			assert.Equal(t, uint64(1), count)
		})
	}
}
