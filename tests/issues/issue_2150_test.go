package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2150 verifies that queries over HTTP with LZ4 or ZSTD compression still work after
// SET readonly = 1 in a session. The server rejects a request that changes
// network_compression_method or network_zstd_compression_level in readonly mode.
// See https://github.com/ClickHouse/clickhouse-go/issues/2150.
func Test2150(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	ctx := context.Background()

	const query = "SELECT toUInt8(getSetting('readonly'))"
	testCases := []struct {
		name   string
		method clickhouse.CompressionMethod
		level  int
		set    string
	}{
		{name: "lz4", method: clickhouse.CompressionLZ4},
		{name: "zstd", method: clickhouse.CompressionZSTD},
		{name: "zstd level 9", method: clickhouse.CompressionZSTD, level: 9},
		{name: "lz4 comments", method: clickhouse.CompressionLZ4, set: "/* a */ -- b\nSET readonly = 1"},
	}
	for _, tc := range testCases {
		if tc.set == "" {
			tc.set = "SET readonly = 1"
		}
		options := func() *clickhouse.Options {
			opts := clickhouse_tests.ClientOptionsFromEnv(env, clickhouse.Settings{
				"session_id": fmt.Sprintf("issue_2150_%s", clickhouse_tests.RandAsciiString(8)),
			}, true)
			opts.Compression = &clickhouse.Compression{Method: tc.method, Level: tc.level}
			opts.MaxOpenConns = 1
			return &opts
		}

		t.Run(tc.name, func(t *testing.T) {
			conn, err := clickhouse.Open(options())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			require.NoError(t, conn.Exec(ctx, tc.set))
			for i := 0; i < 3; i++ {
				var readOnly uint8
				require.NoError(t, conn.QueryRow(ctx, query).Scan(&readOnly))
				assert.Equal(t, uint8(1), readOnly)
			}
		})

		t.Run(tc.name+" std", func(t *testing.T) {
			db := clickhouse.OpenDB(options())
			t.Cleanup(func() { db.Close() })

			// Reserve one connection, so that every query uses the same session.
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			_, err = conn.ExecContext(ctx, tc.set)
			require.NoError(t, err)
			for i := 0; i < 3; i++ {
				var readOnly uint8
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&readOnly))
				assert.Equal(t, uint8(1), readOnly)
			}
		})
	}
}
