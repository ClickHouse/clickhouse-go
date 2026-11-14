package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Issue 1997: one batch INSERT can show two rows in `system.query_log`. ClickHouse 26.2
// and later runs inserts as async inserts by default (`async_insert=1`). The server logs
// the INSERT of the client under the `query_id` of the client. It logs the flush of the
// async insert under a new `query_id`, which the client cannot set. Only one write reaches
// the table.
//
// A caller that needs one `query_id` for a batch sets `async_insert=0` in `Options.Settings`
// or in `clickhouse.WithSettings` on the context of the batch. These tests set it in both
// ways. The test suite sets `async_insert=0` on its own connections, so the tests remove it
// from the client settings first. Then the server default applies, unless the test sets
// `async_insert` itself.
//
// On a server before 26.2, `async_insert` is off by default, and the tests pass without the
// setting.
// See https://github.com/ClickHouse/clickhouse-go/issues/1997.

const rows1997 = 10

// syncBatchCase1997 is one way to set `async_insert=0` for a batch.
type syncBatchCase1997 struct {
	name string
	// connectionSettings is added to the settings of the client.
	connectionSettings clickhouse.Settings
	// batchOptions is added to the context of the batch.
	batchOptions []clickhouse.QueryOption
}

func syncBatchCases1997() []syncBatchCase1997 {
	return []syncBatchCase1997{
		{
			name:               "connection setting",
			connectionSettings: clickhouse.Settings{"async_insert": 0},
		},
		{
			name:         "batch setting",
			batchOptions: []clickhouse.QueryOption{clickhouse.WithSettings(clickhouse.Settings{"async_insert": 0})},
		},
	}
}

// TestIssue1997 checks that a batch from the ClickHouse API logs one `query_id` on the
// Native and the HTTP protocol.
func TestIssue1997(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			for _, tc := range syncBatchCases1997() {
				t.Run(tc.name, func(t *testing.T) {
					opts := clientOptions1997(env, protocol, tc.connectionSettings)
					conn, err := clickhouse.Open(&opts)
					require.NoError(t, err)
					t.Cleanup(func() { conn.Close() })

					table := createTable1997(t, conn)
					queryID := "issue-1997-" + uuid.NewString()
					ctx := batchContext1997(queryID, tc.batchOptions)

					batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, value)", table))
					require.NoError(t, err)
					defer batch.Close()

					for i := 0; i < rows1997; i++ {
						require.NoError(t, batch.Append(uint64(i), fmt.Sprintf("row-%d", i)))
					}
					require.NoError(t, batch.Send())

					requireRows1997(t, conn, table)
					requireSingleInsertQueryID1997(t, conn, table, queryID)
				})
			}
		})
	}
}

// TestIssue1997Std checks that a batch from the `database/sql` API (`Begin`,
// `PrepareContext`, `Commit`) logs one `query_id` on the Native and the HTTP protocol. A
// batch uses only the context that the test gives to `PrepareContext`.
func TestIssue1997Std(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			for _, tc := range syncBatchCases1997() {
				t.Run(tc.name, func(t *testing.T) {
					opts := clientOptions1997(env, protocol, tc.connectionSettings)
					db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
					require.NoError(t, err)
					t.Cleanup(func() { db.Close() })

					// The admin connection uses the ClickHouse API and the settings of the
					// test suite, so the table, the row count and the log do not depend on
					// the async insert default of the server.
					admin, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(env), nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { admin.Close() })

					table := createTable1997(t, admin)
					queryID := "issue-1997-std-" + uuid.NewString()
					ctx := batchContext1997(queryID, tc.batchOptions)

					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err)
					stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (id, value)", table))
					require.NoError(t, err)
					defer stmt.Close()

					for i := 0; i < rows1997; i++ {
						_, err = stmt.ExecContext(ctx, uint64(i), fmt.Sprintf("row-%d", i))
						require.NoError(t, err)
					}
					require.NoError(t, tx.Commit())

					requireRows1997(t, admin, table)
					requireSingleInsertQueryID1997(t, admin, table, queryID)
				})
			}
		})
	}
}

// clientOptions1997 returns the client options of the test suite without `async_insert`,
// so the server default applies. The settings in extra are added to the client settings.
func clientOptions1997(env clickhouse_tests.ClickHouseTestEnvironment, protocol clickhouse.Protocol, extra clickhouse.Settings) clickhouse.Options {
	settings := clickhouse_tests.TestClientDefaultSettings(env)
	delete(settings, "async_insert")
	for key, value := range extra {
		settings[key] = value
	}
	return clickhouse_tests.ClientOptionsFromEnv(env, settings, protocol == clickhouse.HTTP)
}

// batchContext1997 returns the context for a batch: the query ID and the options of the case.
func batchContext1997(queryID string, options []clickhouse.QueryOption) context.Context {
	options = append([]clickhouse.QueryOption{clickhouse.WithQueryID(queryID)}, options...)
	return clickhouse.Context(context.Background(), options...)
}

// createTable1997 creates the test table and drops it when the test ends. It returns the
// name of the table.
func createTable1997(t *testing.T, conn driver.Conn) string {
	t.Helper()

	table := fmt.Sprintf("issue_1997_%s", uuid.NewString()[:8])
	require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf(
		`CREATE TABLE %s (id UInt64, value String) ENGINE = MergeTree() ORDER BY id`, table)))
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Logf("DROP TABLE %s failed: %v", table, err)
		}
	})
	return table
}

// requireRows1997 fails the test unless the table has the rows of the batch.
func requireRows1997(t *testing.T, conn driver.Conn, table string) {
	t.Helper()

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&count))
	require.Equal(t, uint64(rows1997), count, "rows in table %s", table)
}

// requireSingleInsertQueryID1997 fails the test unless the INSERT queries on the table
// are in `system.query_log` under one `query_id`, and that `query_id` is queryID. An async
// insert adds the flush of the server under a second `query_id`.
func requireSingleInsertQueryID1997(t *testing.T, conn driver.Conn, table, queryID string) {
	t.Helper()

	// `system.query_log` is local to each replica. On a ClickHouse Cloud service with more
	// than one replica, the INSERT and the log query can go to different replicas, so the
	// test flushes and reads the logs of all replicas.
	queryLog := "system.query_log"
	flushLogs := "SYSTEM FLUSH LOGS"
	if clickhouse_tests.CloudClickHouse {
		queryLog = "clusterAllReplicas('default', system.query_log)"
		flushLogs = "SYSTEM FLUSH LOGS ON CLUSTER 'default'"
	}

	// The server can send the response before it adds the `QueryFinish` row of the INSERT to
	// the `system.query_log` queue, and then `SYSTEM FLUSH LOGS` does not write that row. The
	// test flushes the logs again until it finds the row of the INSERT.
	//
	// `SYSTEM FLUSH LOGS ON CLUSTER` can wait up to `distributed_ddl_task_timeout` (180 s by
	// default). The timeout of each attempt stops one slow replica from using all of the
	// time, and it gives an error that tells which call was slow.
	//
	// The query reads only INSERT queries: the text of this query has the name of the table.
	var queryIDs []string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(c, conn.Exec(ctx, flushLogs))

		rows, err := conn.Query(ctx, fmt.Sprintf(`
			SELECT DISTINCT query_id
			FROM %s
			WHERE type = 'QueryFinish'
				AND startsWith(upper(trimLeft(query)), 'INSERT')
				AND query ILIKE '%%%s%%'
			ORDER BY query_id
		`, queryLog, table))
		require.NoError(c, err)
		defer rows.Close()

		var ids []string
		for rows.Next() {
			var id string
			require.NoError(c, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(c, rows.Err())
		require.Contains(c, ids, queryID, "`system.query_log` has no INSERT row with the query_id of the batch")
		queryIDs = ids
	}, 30*time.Second, 200*time.Millisecond)

	require.Equal(t, []string{queryID}, queryIDs,
		"a batch INSERT must be in `system.query_log` under one query_id; an extra query_id is the flush of an async insert")
}
