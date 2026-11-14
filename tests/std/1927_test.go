package std

import (
	"crypto/tls"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestStd1927DateQueryParameters covers the database/sql surface of #1927:
// the std driver reaches the same client-side bind rewrite as the native
// driver, so a time.Time sent for a `{d:Date}` parameter through sql.Named or
// clickhouse.DateNamed must be sent as the calendar date, not epoch seconds.
func TestStd1927DateQueryParameters(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}

	d := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := GetStdOpenDBConnection(protocol, nil, tlsConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			// Server-side query parameters require ClickHouse 22.8+; skip
			// (rather than fail) on older servers, matching the native test.
			if !CheckMinServerVersion(conn, 22, 8, 0) {
				t.Skip("server-side query parameters require ClickHouse 22.8+")
			}

			t.Run("Named Date", func(t *testing.T) {
				var got time.Time
				require.NoError(t, conn.QueryRow("SELECT {d:Date}", clickhouse.Named("d", d)).Scan(&got))
				require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
			})

			t.Run("Named Date through sql.Named", func(t *testing.T) {
				var got time.Time
				require.NoError(t, conn.QueryRow("SELECT {d:Date}", sql.Named("d", d)).Scan(&got))
				require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
			})

			t.Run("DateNamed Date", func(t *testing.T) {
				var got time.Time
				require.NoError(t, conn.QueryRow("SELECT {d:Date}", clickhouse.DateNamed("d", d, clickhouse.Seconds)).Scan(&got))
				require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
			})

			t.Run("DateNamed Date32", func(t *testing.T) {
				var got time.Time
				require.NoError(t, conn.QueryRow("SELECT {d:Date32}", clickhouse.DateNamed("d", d, clickhouse.Seconds)).Scan(&got))
				require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
			})

			t.Run("Type name is case-insensitive", func(t *testing.T) {
				for _, decl := range []string{"date", "DATE", "Date32", "DATE32"} {
					t.Run(decl, func(t *testing.T) {
						var got time.Time
						require.NoError(t, conn.QueryRow("SELECT {d:"+decl+"}", clickhouse.Named("d", d)).Scan(&got))
						require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
					})
				}
			})

			t.Run("Nullable(Date)", func(t *testing.T) {
				var got time.Time
				require.NoError(t, conn.QueryRow("SELECT {d:Nullable(Date)}", clickhouse.Named("d", d)).Scan(&got))
				require.Equal(t, "2026-05-15", got.Format("2006-01-02"))
			})
		})
	}
}
