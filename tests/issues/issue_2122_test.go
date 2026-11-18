package issues

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2122 verifies that QueryFormat with the `Native` format over HTTP returns the
// same bytes as the server's own `FORMAT Native`, so that InsertFormat with the
// `Native` format accepts them. The connection sends client_protocol_version on every
// request, and with it the server writes the TCP variant of `Native`, which `Native`
// input does not read.
// See https://github.com/ClickHouse/clickhouse-go/issues/2122.
func Test2122(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	conn, err := clickhouse_tests.GetConnection(testSet, t, clickhouse.HTTP, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx := context.Background()

	queryNative := func(t *testing.T, ctx context.Context, query string) []byte {
		t.Helper()
		stream, err := conn.QueryFormat(ctx, "Native", query)
		require.NoError(t, err)
		data, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		return data
	}

	t.Run("output is standard Native", func(t *testing.T) {
		// One column and one row, then the column name, the type name and the value.
		want := []byte{0x01, 0x01, 0x01, 'x', 0x05, 'U', 'I', 'n', 't', '8', 0x01}
		assert.Equal(t, want, queryNative(t, ctx, "SELECT toUInt8(1) AS x"))
	})

	t.Run("client_protocol_version from WithSettings is kept", func(t *testing.T) {
		// With a TCP revision the server writes the block info before the block and a
		// serialization kind byte before the column data.
		want := []byte{
			0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00,
			0x01, 0x01, 0x01, 'x', 0x05, 'U', 'I', 'n', 't', '8',
			0x00, 0x01,
		}
		queryCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"client_protocol_version": 54460,
		}))
		assert.Equal(t, want, queryNative(t, queryCtx, "SELECT toUInt8(1) AS x"))
	})

	t.Run("InsertFormat reads QueryFormat output", func(t *testing.T) {
		const tableName = "test_2122"
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id UInt64, s String) Engine MergeTree() ORDER BY id", tableName)))
		t.Cleanup(func() {
			if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
				t.Logf("DROP TABLE %s failed: %v", tableName, err)
			}
		})

		data := queryNative(t, ctx, "SELECT number AS id, toString(number) AS s FROM numbers(1000)")
		require.NoError(t, conn.InsertFormat(ctx, "Native", fmt.Sprintf("INSERT INTO %s (id, s)", tableName), bytes.NewReader(data)))

		var count, sum, matching uint64
		require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT count(), sum(id), countIf(s = toString(id)) FROM %s", tableName)).Scan(&count, &sum, &matching))
		assert.Equal(t, uint64(1000), count)
		assert.Equal(t, uint64(499500), sum)
		assert.Equal(t, uint64(1000), matching)
	})
}
