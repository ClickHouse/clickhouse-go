package issues

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// Test2067 verifies that a batch insert (PrepareBatch, AppendStruct, Send)
// runs as an async insert on the server when the caller asks for it, through
// clickhouse.WithAsync on the context, through settings on the context, or through
// settings on the client. The server is the source of truth: the test reads the
// `AsyncInsertQuery` profile event of the INSERT from `system.query_log`.
// See https://github.com/ClickHouse/clickhouse-go/issues/2067.
func Test2067(t *testing.T) {
	// The server rejects `async_insert` together with `insert_quorum` unless
	// `insert_quorum_parallel=1`, and the test helpers force `insert_quorum_parallel=0`.
	t.Setenv("CLICKHOUSE_QUORUM_INSERT", "0")

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
		return settings
	}

	testCases := []struct {
		name     string
		settings clickhouse.Settings
		ctx      func() context.Context
		opts     []driver.PrepareBatchOption
		// flush reports whether the test calls Flush after half of the rows.
		flush bool
		// nativeInserts is the number of INSERT queries that the Native batch sends.
		// Zero means one. The HTTP batch always sends one INSERT.
		nativeInserts int
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
			// The batch releases the connection after PrepareBatch, and Send sends the
			// INSERT again on a new connection.
			name:     "context WithAsync wait release connection",
			settings: clickhouse_tests.TestClientDefaultSettings(testEnv),
			ctx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithAsync(true))
			},
			opts: []driver.PrepareBatchOption{driver.WithReleaseConnection()},
			// PrepareBatch sends an INSERT without data, and Send sends the INSERT again.
			nativeInserts: 2,
			wait:          true,
		},
		{
			// Flush ends the INSERT and releases the connection, and Send sends the
			// INSERT again on a new connection.
			name:     "context WithAsync wait close on flush",
			settings: clickhouse_tests.TestClientDefaultSettings(testEnv),
			ctx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithAsync(true))
			},
			opts:  []driver.PrepareBatchOption{driver.WithCloseOnFlush()},
			flush: true,
			// Flush ends the first INSERT, and Send sends the second INSERT.
			nativeInserts: 2,
			wait:          true,
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
					"async_insert":          1,
					"wait_for_async_insert": 1,
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
					createTable2067(t, admin, tableName)

					queryID := fmt.Sprintf("test-2067-%v-%d-%d", protocol, i, time.Now().UnixNano())
					ctx := clickhouse.Context(tc.ctx(), clickhouse.WithQueryID(queryID))

					batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName), tc.opts...)
					require.NoError(t, err)
					for n := range rowsPerInsert {
						require.NoError(t, batch.AppendStruct(&row{Col1: uint64(n), Col2: "value"}))
						if tc.flush && n == rowsPerInsert/2-1 {
							require.NoError(t, batch.Flush())
						}
					}
					require.NoError(t, batch.Send())

					inserts := 1
					if protocol == clickhouse.Native && tc.nativeInserts > 0 {
						inserts = tc.nativeInserts
					}
					requireAsyncInsert(t, admin, queryID, inserts)
					requireRows2067(t, admin, tableName, rowsPerInsert, tc.wait)
				})
			}
		})
	}
}

// Test2067Std verifies that a `database/sql` batch insert (Begin, PrepareContext,
// Exec, Commit) runs as an async insert on the server when the context of
// PrepareContext has clickhouse.WithAsync.
// See https://github.com/ClickHouse/clickhouse-go/issues/2067.
func Test2067Std(t *testing.T) {
	// See Test2067.
	t.Setenv("CLICKHOUSE_QUORUM_INSERT", "0")

	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	const rowsPerInsert = 10

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			for i, wait := range []bool{true, false} {
				t.Run(fmt.Sprintf("WithAsync %v", wait), func(t *testing.T) {
					db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
					require.NoError(t, err)
					t.Cleanup(func() { db.Close() })

					// The admin connection uses the native API, so that the checks are the
					// same as in Test2067.
					admin, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { admin.Close() })

					if !clickhouse_tests.CheckMinServerServerVersion(admin, 22, 8, 0) {
						t.Skip("async insert profile events in `system.query_log` need ClickHouse 22.8+")
					}

					tableName := fmt.Sprintf("test_2067_std_%v_%d", protocol, i)
					createTable2067(t, admin, tableName)

					queryID := fmt.Sprintf("test-2067-std-%v-%d-%d", protocol, i, time.Now().UnixNano())
					ctx := clickhouse.Context(context.Background(), clickhouse.WithAsync(wait), clickhouse.WithQueryID(queryID))

					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err)
					stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
					require.NoError(t, err)
					for n := range rowsPerInsert {
						_, err := stmt.ExecContext(ctx, uint64(n), "value")
						require.NoError(t, err)
					}
					require.NoError(t, tx.Commit())

					requireAsyncInsert(t, admin, queryID, 1)
					requireRows2067(t, admin, tableName, rowsPerInsert, wait)
				})
			}
		})
	}
}

// createTable2067 creates the test table and drops it when the test ends.
func createTable2067(t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()

	require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s
	(
		col1 UInt64,
		col2 String
	)
	ENGINE = MergeTree
	ORDER BY col1`, tableName)), "Create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Logf("DROP TABLE %s failed: %v", tableName, err)
		}
	})
}

// requireRows2067 fails the test unless the table has the given number of rows. When
// the insert does not wait for the async flush, the rows become visible some time after
// the insert returns, so the test waits for them.
func requireRows2067(t *testing.T, conn driver.Conn, tableName string, rows uint64, wait bool) {
	t.Helper()

	countRows := func() uint64 {
		var count uint64
		require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
		return count
	}
	if wait {
		require.Equal(t, rows, countRows())
		return
	}
	require.Eventually(t, func() bool { return countRows() == rows }, 30*time.Second, 200*time.Millisecond,
		"async insert rows did not become visible")
}

// requireAsyncInsert fails the test unless the server ran all of the given number of
// INSERT queries with the given query ID as async inserts. A batch can send more than
// one INSERT with the same query ID, for example after Flush with driver.WithCloseOnFlush.
func requireAsyncInsert(t *testing.T, conn driver.Conn, queryID string, inserts int) {
	t.Helper()

	// `system.query_log` is local to each replica. On a ClickHouse Cloud service with more
	// than one replica, the INSERT and the admin connection can go to different replicas,
	// so the test reads the logs of all replicas. `SYSTEM FLUSH LOGS` flushes only the
	// local replica, and the other replicas can wait more than 30 seconds before they
	// write their logs, so the test flushes the logs of all replicas.
	queryLog := "system.query_log"
	flushLogs := "SYSTEM FLUSH LOGS"
	if clickhouse_tests.CloudClickHouse {
		queryLog = "clusterAllReplicas('default', system.query_log)"
		flushLogs = "SYSTEM FLUSH LOGS ON CLUSTER 'default'"
	}

	// The server can send the HTTP response before it adds the `QueryFinish` row to the
	// `system.query_log` queue. Then `SYSTEM FLUSH LOGS` does not write the row, so the
	// test flushes the logs again until all rows are there.
	//
	// The `Settings` column is not checked: it lists only settings that differ from the
	// server default, and newer servers default to `async_insert=1`.
	var gotInserts, syncInserts uint64
	//
	// `SYSTEM FLUSH LOGS ON CLUSTER` can wait up to `distributed_ddl_task_timeout` (180 s by
	// default). The timeout on each attempt stops one slow replica from using all of the
	// time, and gives an error that tells which call was slow.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(c, conn.Exec(ctx, flushLogs))
		err := conn.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(), countIf(ProfileEvents['AsyncInsertQuery'] = 0)
			FROM %s
			WHERE query_id = ? AND type = 'QueryFinish' AND query_kind = 'Insert'
		`, queryLog), queryID).Scan(&gotInserts, &syncInserts)
		require.NoError(c, err)
		require.Equal(c, uint64(inserts), gotInserts, "`system.query_log` rows for query_id=%s", queryID)
	}, 30*time.Second, 200*time.Millisecond)

	require.Zero(t, syncInserts, "the server ran %d of %d INSERT queries as sync inserts", syncInserts, inserts)
}
