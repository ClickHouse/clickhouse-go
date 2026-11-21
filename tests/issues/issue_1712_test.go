package issues

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1712 verifies that a time.Time bound client-side keeps the moment
// it points to when the session_timezone setting differs from the server
// timezone. The driver used to write a value in the server timezone without
// a timezone, and the server then read it in the session timezone.
// See https://github.com/ClickHouse/clickhouse-go/issues/1712.
func TestIssue1712(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
		require.NoError(t, err)
		clickhouse_tests.CleanupNativeConn(t, conn)

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 23, 6, 0) {
			t.Skip("session_timezone requires ClickHouse 23.6+")
		}

		var serverTimezone string
		require.NoError(t, conn.QueryRow(ctx, "SELECT serverTimezone()").Scan(&serverTimezone))
		sessionTimezone := "America/New_York"
		if serverTimezone == sessionTimezone {
			sessionTimezone = "Europe/Warsaw"
		}
		loc, err := time.LoadLocation(serverTimezone)
		require.NoError(t, err)

		const ddl = `
			CREATE TABLE test_issue_1712 (
				  dt   DateTime
				, dt64 DateTime64(3)
			) Engine MergeTree ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1712"))
		require.NoError(t, conn.Exec(ctx, ddl))
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1712"))
		})

		value := time.Date(2025, 11, 12, 16, 33, 21, 0, loc)
		sessionCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"session_timezone": sessionTimezone,
		}))
		require.NoError(t, conn.Exec(sessionCtx, "INSERT INTO test_issue_1712 VALUES (?, ?)", value, value))

		var (
			dt   int64
			dt64 int64
		)
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT toInt64(toUnixTimestamp(dt)), toUnixTimestamp64Milli(dt64) FROM test_issue_1712",
		).Scan(&dt, &dt64))
		require.Equal(t, value.Unix(), dt)
		require.Equal(t, value.UnixMilli(), dt64)
	})
}
