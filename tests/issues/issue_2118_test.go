package issues

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2118 verifies that a user with readonly = 1 can query over HTTP with LZ4 or ZSTD
// compression. The server rejects a request from such a user when it changes
// network_compression_method or network_zstd_compression_level.
// See https://github.com/ClickHouse/clickhouse-go/issues/2118.
func Test2118(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	ctx := context.Background()

	admin, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	username := fmt.Sprintf("readonly_user_%s", clickhouse_tests.RandAsciiString(6))
	password := clickhouse_tests.RandAsciiString(10) + "1#"
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s' SETTINGS readonly = 1", username, password)))
	t.Cleanup(func() {
		if err := admin.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", username)); err != nil {
			t.Logf("DROP USER %s failed: %v", username, err)
		}
	})

	testCases := []struct {
		name   string
		method clickhouse.CompressionMethod
		level  int
	}{
		{name: "lz4", method: clickhouse.CompressionLZ4},
		{name: "zstd", method: clickhouse.CompressionZSTD},
		{name: "zstd level 1", method: clickhouse.CompressionZSTD, level: 1},
		{name: "zstd level 9", method: clickhouse.CompressionZSTD, level: 9},
	}
	for _, tc := range testCases {
		opts := clickhouse_tests.ClientOptionsFromEnv(env, nil, true)
		opts.Auth.Username = username
		opts.Auth.Password = password
		opts.Compression = &clickhouse.Compression{Method: tc.method, Level: tc.level}
		// A longer dial timeout adds max_execution_time to the server hello query, and the
		// server rejects that for a read-only user too (#1748).
		opts.DialTimeout = time.Second

		t.Run(tc.name, func(t *testing.T) {
			conn, err := clickhouse.Open(&opts)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			require.NoError(t, conn.Ping(ctx))
			var sum uint64
			require.NoError(t, conn.QueryRow(ctx, "SELECT sum(arrayJoin(range(100000)))").Scan(&sum))
			assert.Equal(t, uint64(4999950000), sum)

			rows, err := conn.Query(ctx, "SELECT toString(arrayJoin(range(100000)))")
			require.NoError(t, err)
			t.Cleanup(func() { rows.Close() })
			var count int
			for rows.Next() {
				var s string
				require.NoError(t, rows.Scan(&s))
				count++
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, 100000, count)
		})

		t.Run(tc.name+" std", func(t *testing.T) {
			db := clickhouse.OpenDB(&opts)
			t.Cleanup(func() { db.Close() })

			var one uint8
			require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
			assert.Equal(t, uint8(1), one)
		})
	}
}

// Test2118_ConnectionSettings verifies that network_compression_method and
// network_zstd_compression_level from Options.Settings are not replaced by the values
// from Options.Compression.
func Test2118_ConnectionSettings(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionHTTP("issues", t.Name(), clickhouse.Settings{
		"network_compression_method":     "LZ4",
		"network_zstd_compression_level": 2,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionZSTD,
		Level:  9,
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx := context.Background()

	var method string
	require.NoError(t, conn.QueryRow(ctx, "SELECT getSetting('network_compression_method')").Scan(&method))
	assert.Equal(t, "LZ4", strings.ToUpper(method))
	var level int8
	require.NoError(t, conn.QueryRow(ctx, "SELECT getSetting('network_zstd_compression_level')").Scan(&level))
	assert.Equal(t, int8(2), level)

	// A setting for one query still has priority over the connection setting.
	queryCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"network_zstd_compression_level": 5}))
	require.NoError(t, conn.QueryRow(queryCtx, "SELECT getSetting('network_zstd_compression_level')").Scan(&level))
	assert.Equal(t, int8(5), level)
}
