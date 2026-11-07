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

// Test2100 verifies that a `max_execution_time` set on the context is kept when the context
// deadline is later than it. Before the fix, the driver replaced it with the deadline + 5s, so
// the query ran past the limit.
// See https://github.com/ClickHouse/clickhouse-go/issues/2100.
func Test2100(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_execution_time": 1}))
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			t.Cleanup(cancel)

			start := time.Now()
			err = conn.Exec(ctx, "SELECT sleepEachRow(1) FROM numbers(5) SETTINGS max_block_size = 1")
			require.Error(t, err)
			// 159 is TIMEOUT_EXCEEDED.
			assert.Contains(t, strings.ToLower(err.Error()), "code: 159")
			assert.Less(t, time.Since(start), 5*time.Second)
		})
	}
}
