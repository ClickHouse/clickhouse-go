package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue1938_SpacedQueryParameter(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			t.Skip("server-side query parameters require ClickHouse 22.8+")
		}

		var actual uint64
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT { value : UInt64 }",
			clickhouse.Named("value", 42),
		).Scan(&actual))
		require.Equal(t, uint64(42), actual)
	})
}

func TestIssue1938_QuotedJSONWithPositionalArgument(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		var literal string
		var bound uint8
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT '{"key":"value"}', ?`, 7,
		).Scan(&literal, &bound))
		require.Equal(t, `{"key":"value"}`, literal)
		require.Equal(t, uint8(7), bound)
	})
}
