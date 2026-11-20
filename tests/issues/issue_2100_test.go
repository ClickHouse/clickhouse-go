package issues

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2100 verifies that the context deadline does not change the `max_execution_time` the
// server uses.
// See https://github.com/ClickHouse/clickhouse-go/issues/2100.
func Test2100(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	limit := clickhouse.Settings{"max_execution_time": 2}
	cases := []struct {
		name         string
		ctxSettings  clickhouse.Settings
		connSettings clickhouse.Settings
	}{
		{name: "not set"},
		{name: "context", ctxSettings: limit},
		{name: "connection", connSettings: limit},
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v/%s", protocol, tc.name), func(t *testing.T) {
				settings := clickhouse_tests.TestClientDefaultSettings(testEnv)
				maps.Copy(settings, tc.connSettings)
				conn, err := clickhouse_tests.GetConnection("issues", t, protocol, settings, nil, nil)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(tc.ctxSettings))
				deadlineCtx, cancel := context.WithTimeout(ctx, time.Minute)
				t.Cleanup(cancel)

				const query = "SELECT toString(getSetting('max_execution_time'))"
				var want, got string
				require.NoError(t, conn.QueryRow(ctx, query).Scan(&want))
				require.NoError(t, conn.QueryRow(deadlineCtx, query).Scan(&got))
				require.Equal(t, want, got)
			})
		}
	}
}
