package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// asyncInsertCase is one way for a caller to control async insert. The client opens
// its connection without the async insert override of the test helpers, so a case
// without settings gets the server default.
type asyncInsertCase struct {
	name string
	// clientSettings are the settings of the client. Nil means no settings.
	clientSettings clickhouse.Settings
	// ctxSettings are the settings on the context of the INSERT. Nil means none.
	ctxSettings clickhouse.Settings
	// serverDefault reports whether the case depends on the server default.
	serverDefault bool
	// async reports whether the server must run the INSERT as an async insert.
	async bool
}

var asyncInsertCases = []asyncInsertCase{
	{
		// ClickHouse 26.2 and later enable async insert by default.
		name:          "server default",
		serverDefault: true,
		async:         true,
	},
	{
		name:           "client async_insert=0",
		clientSettings: clickhouse.Settings{"async_insert": 0},
		async:          false,
	},
	{
		name:        "context async_insert=0",
		ctxSettings: clickhouse.Settings{"async_insert": 0},
		async:       false,
	},
}

// TestAsyncInsertSettings verifies that a batch insert runs as an async insert with
// the server default, and as a sync insert when the caller sets `async_insert=0` on the
// client or on the context. The server is the source of truth: the test reads the
// `AsyncInsertQuery` profile event of the INSERT from `system.query_log`.
func TestAsyncInsertSettings(t *testing.T) {
	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	const rowsPerInsert = 10

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		for i, tc := range asyncInsertCases {
			t.Run(tc.name, func(t *testing.T) {
				admin := asyncInsertAdmin(t, env, protocol, tc)

				// Do not use GetConnection: it sets `async_insert=0` when the settings do
				// not have it, and the test must see the server default.
				opts := ClientOptionsFromEnv(env, tc.clientSettings, protocol == clickhouse.HTTP)
				conn, err := clickhouse.Open(&opts)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				tableName := fmt.Sprintf("test_async_insert_settings_%v_%d", protocol, i)
				createAsyncInsertTable(t, admin, tableName)

				queryID := fmt.Sprintf("test-async-insert-settings-%v-%d-%d", protocol, i, time.Now().UnixNano())
				ctx := asyncInsertContext(tc, queryID)

				batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
				require.NoError(t, err)
				for n := range rowsPerInsert {
					require.NoError(t, batch.Append(uint64(n), "value"))
				}
				require.NoError(t, batch.Send())

				requireInsertKind(t, admin, queryID, tc.async)
				requireAsyncInsertRows(t, admin, tableName, rowsPerInsert)
			})
		}
	})
}

// TestAsyncInsertSettingsStd is the `database/sql` variant of TestAsyncInsertSettings.
// It uses ExecContext without a transaction, which sends the INSERT through Exec.
func TestAsyncInsertSettingsStd(t *testing.T) {
	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		for i, tc := range asyncInsertCases {
			t.Run(tc.name, func(t *testing.T) {
				admin := asyncInsertAdmin(t, env, protocol, tc)

				// Do not use the std test helpers: they set `async_insert=0` when the
				// settings do not have it, and the test must see the server default.
				opts := ClientOptionsFromEnv(env, tc.clientSettings, protocol == clickhouse.HTTP)
				db := clickhouse.OpenDB(&opts)
				t.Cleanup(func() { db.Close() })

				tableName := fmt.Sprintf("test_async_insert_settings_std_%v_%d", protocol, i)
				createAsyncInsertTable(t, admin, tableName)

				queryID := fmt.Sprintf("test-async-insert-settings-std-%v-%d-%d", protocol, i, time.Now().UnixNano())
				ctx := asyncInsertContext(tc, queryID)

				_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), uint64(1), "value")
				require.NoError(t, err)

				requireInsertKind(t, admin, queryID, tc.async)
				requireAsyncInsertRows(t, admin, tableName, 1)
			})
		}
	})
}

// asyncInsertAdmin opens the admin connection and skips the test when the server is too
// old for the case. The admin connection uses the default test settings, so that the
// setup and the checks are synchronous whatever the case asks for.
func asyncInsertAdmin(t *testing.T, env ClickHouseTestEnvironment, protocol clickhouse.Protocol, tc asyncInsertCase) driver.Conn {
	t.Helper()

	admin, err := GetConnection(testSet, t, protocol, TestClientDefaultSettings(env), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	if !CheckMinServerServerVersion(admin, 22, 8, 0) {
		t.Skip("async insert profile events in `system.query_log` need ClickHouse 22.8+")
	}
	if tc.serverDefault && !CheckMinServerServerVersion(admin, 26, 2, 0) {
		t.Skip("async insert is the server default on ClickHouse 26.2+")
	}
	return admin
}

// asyncInsertContext returns the context of the INSERT for the case.
func asyncInsertContext(tc asyncInsertCase, queryID string) context.Context {
	opts := []clickhouse.QueryOption{clickhouse.WithQueryID(queryID)}
	if tc.ctxSettings != nil {
		opts = append(opts, clickhouse.WithSettings(tc.ctxSettings))
	}
	return clickhouse.Context(context.Background(), opts...)
}

// createAsyncInsertTable creates the test table and drops it when the test ends.
func createAsyncInsertTable(t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()

	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s
	(
		col1 UInt64,
		col2 String
	)
	ENGINE = MergeTree
	ORDER BY col1`, tableName)), "create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Logf("DROP TABLE %s failed: %v", tableName, err)
		}
	})
}

// requireAsyncInsertRows fails the test unless the table has the given number of rows.
// A server profile can set `wait_for_async_insert=0`. Then the rows become visible some
// time after the insert returns, so the test waits for them.
func requireAsyncInsertRows(t *testing.T, conn driver.Conn, tableName string, rows uint64) {
	t.Helper()

	var count uint64
	require.Eventually(t, func() bool {
		err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count)
		return err == nil && count == rows
	}, 30*time.Second, 200*time.Millisecond, "table %s has %d rows, want %d", tableName, count, rows)
}

// requireInsertKind fails the test unless the server ran the INSERT with the given
// query ID as an async insert (async is true) or as a sync insert (async is false).
func requireInsertKind(t *testing.T, conn driver.Conn, queryID string, async bool) {
	t.Helper()

	// `system.query_log` is local to each replica. On a ClickHouse Cloud service with more
	// than one replica, the INSERT and the admin connection can go to different replicas,
	// so the test reads and flushes the logs of all replicas.
	queryLog := "system.query_log"
	flushLogs := "SYSTEM FLUSH LOGS"
	if CloudClickHouse {
		queryLog = "clusterAllReplicas('default', system.query_log)"
		flushLogs = "SYSTEM FLUSH LOGS ON CLUSTER 'default'"
	}

	// The server can send the response before it adds the `QueryFinish` row to the
	// `system.query_log` queue, so the test flushes the logs again until the row is there.
	// The timeout on each attempt stops one slow replica from using all of the time.
	var inserts, asyncInserts uint64
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(c, conn.Exec(ctx, flushLogs))
		err := conn.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(), countIf(ProfileEvents['AsyncInsertQuery'] > 0)
			FROM %s
			WHERE query_id = ? AND type = 'QueryFinish' AND query_kind = 'Insert'
		`, queryLog), queryID).Scan(&inserts, &asyncInserts)
		require.NoError(c, err)
		require.Equal(c, uint64(1), inserts, "`system.query_log` rows for query_id=%s", queryID)
	}, 30*time.Second, 200*time.Millisecond)

	if async {
		require.Equal(t, uint64(1), asyncInserts, "the server ran the INSERT as a sync insert, want async")
	} else {
		require.Zero(t, asyncInserts, "the server ran the INSERT as an async insert, want sync")
	}
}
