package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2067 verifies that a batch insert (PrepareBatch, AppendStruct, Send)
// runs as an async insert on the server when the caller asks for it, through
// clickhouse.WithAsync on the context, through settings on the context, or through
// settings on the client. The server is the source of truth: the test reads the
// `AsyncInsertQuery` profile event of the INSERT from `system.query_log`.
// See https://github.com/ClickHouse/clickhouse-go/issues/2067.
func Test2067(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	type row struct {
		Col1 uint64 `ch:"col1"`
		Col2 string `ch:"col2"`
	}
	const rowsPerInsert = 10

	asyncClientSettings := func() clickhouse.Settings {
		settings := clickhouse_tests.TestClientDefaultSettings(testEnv)
		settings["async_insert"] = 1
		settings["wait_for_async_insert"] = 1
		// The server rejects `async_insert` together with `insert_quorum` unless
		// `insert_quorum_parallel` is set.
		settings["insert_quorum_parallel"] = 1
		return settings
	}

	testCases := []struct {
		name     string
		settings clickhouse.Settings
		ctx      func() context.Context
		// wait reports whether the insert waits for the async flush. When it does not,
		// the rows become visible some time after Send returns.
		wait bool
	}{
		{
			name:     "context WithAsync wait",
			settings: clickhouse_tests.TestClientDefaultSettings(testEnv),
			ctx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithAsync(true))
			},
			wait: true,
		},
		{
			name:     "context WithAsync no wait",
			settings: clickhouse_tests.TestClientDefaultSettings(testEnv),
			ctx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithAsync(false))
			},
			wait: false,
		},
		{
			name:     "context settings",
			settings: clickhouse_tests.TestClientDefaultSettings(testEnv),
			ctx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
					"async_insert":           1,
					"wait_for_async_insert":  1,
					"insert_quorum_parallel": 1,
				}))
			},
			wait: true,
		},
		{
			name:     "client settings",
			settings: asyncClientSettings(),
			ctx:      context.Background,
			wait:     true,
		},
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			for i, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, tc.settings, nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { conn.Close() })

					if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
						t.Skip("async insert profile events in `system.query_log` need ClickHouse 22.8+")
					}

					// The admin connection uses the default settings, so that the setup and
					// the checks are synchronous whatever the test case asks for.
					admin, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { admin.Close() })

					tableName := fmt.Sprintf("test_2067_%v_%d", protocol, i)
					require.NoError(t, admin.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s
					(
						col1 UInt64,
						col2 String
					)
					ENGINE = MergeTree
					ORDER BY col1`, tableName)), "Create table failed")
					t.Cleanup(func() {
						if err := admin.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
							t.Logf("DROP TABLE %s failed: %v", tableName, err)
						}
					})

					queryID := fmt.Sprintf("test-2067-%v-%d-%d", protocol, i, time.Now().UnixNano())
					ctx := clickhouse.Context(tc.ctx(), clickhouse.WithQueryID(queryID))

					batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
					require.NoError(t, err)
					for n := range rowsPerInsert {
						require.NoError(t, batch.AppendStruct(&row{Col1: uint64(n), Col2: "value"}))
					}
					require.NoError(t, batch.Send())

					requireAsyncInsert(t, admin, queryID)

					countRows := func() uint64 {
						var count uint64
						require.NoError(t, admin.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
						return count
					}
					if tc.wait {
						require.Equal(t, uint64(rowsPerInsert), countRows())
					} else {
						require.Eventually(t, func() bool { return countRows() == rowsPerInsert }, 30*time.Second, 200*time.Millisecond,
							"async insert rows did not become visible")
					}
				})
			}
		})
	}
}

// requireAsyncInsert fails the test unless the server ran the INSERT with the given
// query ID as an async insert.
func requireAsyncInsert(t *testing.T, conn driver.Conn, queryID string) {
	t.Helper()

	require.NoError(t, conn.Exec(context.Background(), "SYSTEM FLUSH LOGS"))

	// The `Settings` column is not checked: it lists only settings that differ from the
	// server default, and newer servers default to `async_insert=1`.
	var asyncInsertQuery uint64
	err := conn.QueryRow(context.Background(), `
		SELECT ProfileEvents['AsyncInsertQuery']
		FROM system.query_log
		WHERE query_id = ? AND type = 'QueryFinish' AND query_kind = 'Insert'
		ORDER BY event_time_microseconds DESC
		LIMIT 1
	`, queryID).Scan(&asyncInsertQuery)
	require.NoError(t, err, "expected a `system.query_log` row for query_id=%s", queryID)

	require.Positive(t, asyncInsertQuery, "the server did not run the INSERT as an async insert")
}
