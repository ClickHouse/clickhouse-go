package issues

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

func TestIssue1942_ByteAndDurationBinding(t *testing.T) {
	ctx := context.Background()
	binary := []byte{'A', 0, 'B'}
	duration := 14*time.Hour + 30*time.Minute + 123*time.Millisecond

	t.Run("native", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.Native, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		var got string
		require.NoError(t, conn.QueryRow(ctx, "SELECT ?", binary).Scan(&got))
		assert.Equal(t, string(binary), got)

		if clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			require.NoError(t, conn.QueryRow(ctx, "SELECT {p:String}", clickhouse.Named("p", binary)).Scan(&got))
			assert.Equal(t, string(binary), got)
		}

		if clickhouse_tests.CheckMinServerServerVersion(conn, 25, 6, 0) {
			timeCtx := ctx
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 12, 0) {
				timeCtx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
					"enable_time_time64_type": 1,
				}))
			}
			require.NoError(t, conn.QueryRow(timeCtx, "SELECT toString(CAST(? AS Time64(3)))", duration).Scan(&got))
			assert.Equal(t, "14:30:00.123", got)
			require.NoError(t, conn.QueryRow(timeCtx, "SELECT toString({p:Time64(3)})", clickhouse.Named("p", duration)).Scan(&got))
			assert.Equal(t, "14:30:00.123", got)
		}
	})

	t.Run("database/sql", func(t *testing.T) {
		useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)
		conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		var got string
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT ?", binary).Scan(&got))
		assert.Equal(t, string(binary), got)

		if clickhouse_std_tests.CheckMinServerVersion(conn, 22, 8, 0) {
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT {p:String}", clickhouse.Named("p", binary)).Scan(&got))
			assert.Equal(t, string(binary), got)
		}

		if clickhouse_std_tests.CheckMinServerVersion(conn, 25, 6, 0) {
			timeConn := conn
			if !clickhouse_std_tests.CheckMinServerVersion(conn, 25, 12, 0) {
				opts := url.Values{}
				opts.Set("enable_time_time64_type", "1")
				timeConn, err = clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, opts)
				require.NoError(t, err)
				defer timeConn.Close()
			}
			require.NoError(t, timeConn.QueryRowContext(ctx, "SELECT toString(CAST(? AS Time64(3)))", duration).Scan(&got))
			assert.Equal(t, "14:30:00.123", got)
			require.NoError(t, timeConn.QueryRowContext(ctx, "SELECT toString({p:Time64(3)})", clickhouse.Named("p", duration)).Scan(&got))
			assert.Equal(t, "14:30:00.123", got)
		}
	})
}
