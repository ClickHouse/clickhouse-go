package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue1948_JSONTypedPathWithSpace(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()

		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse.Settings{
			"allow_experimental_json_type": true,
		}, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
			t.Skip("JSON unsupported on this server version")
		}

		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1948"))
		require.NoError(t, conn.Exec(ctx, "CREATE TABLE test_1948 (data JSON(`a b` Int64)) ENGINE = MergeTree ORDER BY tuple()"))
		t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1948") })

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1948 (data)")
		require.NoError(t, err)
		require.NoError(t, batch.Append(`{"a b": 1}`))
		require.NoError(t, batch.Send())

		var data clickhouse.JSON
		require.NoError(t, conn.QueryRow(ctx, "SELECT data FROM test_1948").Scan(&data))
		got, ok := data.ValueAtPath("a b")
		require.True(t, ok)
		assert.EqualValues(t, 1, got)
	})
}
