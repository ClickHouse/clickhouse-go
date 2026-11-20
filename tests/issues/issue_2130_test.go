package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

const query2130 = "SELECT number AS id, number * 10 AS version FROM numbers(3)"

// formatCases2130 are FORMAT clauses that make the server send the result over HTTP
// in a format other than `Native`.
var formatCases2130 = []struct {
	name   string
	clause string
	// format is the name that the server sends in the `X-ClickHouse-Format` header.
	format string
}{
	{name: "JSONEachRow", clause: " FORMAT JSONEachRow", format: "JSONEachRow"},
	{name: "CSV", clause: " FORMAT CSV", format: "CSV"},
	{name: "RowBinary", clause: " FORMAT RowBinary", format: "RowBinary"},
	// The server sends the name as the query writes it.
	{name: "lowercase", clause: " format jsoneachrow", format: "jsoneachrow"},
	// The FORMAT clause is not at the end of the query.
	{name: "before SETTINGS", clause: " FORMAT JSONEachRow SETTINGS max_threads = 1", format: "JSONEachRow"},
}

// nativeClauses2130 keep the result in the `Native` format.
var nativeClauses2130 = []string{"", " FORMAT Native", " format native"}

type row2130 struct {
	ID      uint64 `ch:"id"`
	Version uint64 `ch:"version"`
}

var wantRows2130 = []row2130{{ID: 0, Version: 0}, {ID: 1, Version: 10}, {ID: 2, Version: 20}}

func formatError2130(format string) string {
	return fmt.Sprintf("clickhouse: the server sent the result in format %s, but Query over HTTP can read only Native, so remove the FORMAT clause from the query or use QueryFormat to read the %s bytes", format, format)
}

// Test2130 verifies that Query over HTTP returns an error that names the format when a
// FORMAT clause makes the server send the result in a format other than `Native`. Before
// the fix, Query decoded the body as `Native`. Text formats gave a decode error that did
// not tell the cause, and `RowBinary` gave an empty result and no error. The native
// protocol returns the rows, because the server does not use the FORMAT clause there.
// See https://github.com/ClickHouse/clickhouse-go/issues/2130.
func Test2130(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	ctx := context.Background()

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			// The HTTP test connection has one connection in the pool.
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			for _, tc := range formatCases2130 {
				t.Run(tc.name, func(t *testing.T) {
					query := query2130 + tc.clause
					if protocol == clickhouse.Native {
						requireRows2130(t, conn, query)
						return
					}

					rows, err := conn.Query(ctx, query)
					if err == nil {
						// Close the rows, so that they do not keep the only connection of the
						// pool and block the next queries.
						require.NoError(t, rows.Close())
					}
					require.EqualError(t, err, formatError2130(tc.format))
					var id, version uint64
					require.EqualError(t, conn.QueryRow(ctx, query).Scan(&id, &version), formatError2130(tc.format))
					var got []row2130
					require.EqualError(t, conn.Select(ctx, &got, query), formatError2130(tc.format))
					assert.Empty(t, got)

					// The connection goes back to the pool and the next query uses it.
					assert.Equal(t, 1, conn.Stats().Idle)
					requireRows2130(t, conn, query2130)
				})
			}

			for _, clause := range nativeClauses2130 {
				t.Run(fmt.Sprintf("Native body %q", clause), func(t *testing.T) {
					requireRows2130(t, conn, query2130+clause)
				})
			}

			t.Run("FORMAT Null", func(t *testing.T) {
				// The server sends no rows for `Null` on both protocols.
				var got []row2130
				require.NoError(t, conn.Select(ctx, &got, query2130+" FORMAT Null"))
				assert.Empty(t, got)
			})

			t.Run("Exec", func(t *testing.T) {
				// Exec does not read the result.
				require.NoError(t, conn.Exec(ctx, query2130+" FORMAT JSONEachRow"))
				requireRows2130(t, conn, query2130)
			})
		})
	}

	// The server sends the header for a compressed body too.
	for _, method := range []clickhouse.CompressionMethod{clickhouse.CompressionLZ4, clickhouse.CompressionGZIP} {
		t.Run(fmt.Sprintf("HTTP %s", method), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, clickhouse.HTTP, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, &clickhouse.Compression{Method: method})
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			rows, err := conn.Query(ctx, query2130+" FORMAT JSONEachRow")
			if err == nil {
				require.NoError(t, rows.Close())
			}
			require.EqualError(t, err, formatError2130("JSONEachRow"))
			assert.Equal(t, 1, conn.Stats().Idle)
			requireRows2130(t, conn, query2130)
		})
	}
}

// Test2130Std verifies the same for `database/sql`.
// See https://github.com/ClickHouse/clickhouse-go/issues/2130.
func Test2130Std(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	ctx := context.Background()

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			// One connection, so that the next query uses the connection that got the error.
			db.SetMaxOpenConns(1)

			for _, tc := range formatCases2130 {
				t.Run(tc.name, func(t *testing.T) {
					query := query2130 + tc.clause
					if protocol == clickhouse.Native {
						requireStdRows2130(t, db, query)
						return
					}

					rows, err := db.QueryContext(ctx, query)
					if err == nil {
						// See Test2130.
						require.NoError(t, rows.Close())
					}
					require.EqualError(t, err, formatError2130(tc.format))
					var id, version uint64
					require.EqualError(t, db.QueryRowContext(ctx, query).Scan(&id, &version), formatError2130(tc.format))

					requireStdRows2130(t, db, query2130)
				})
			}

			for _, clause := range nativeClauses2130 {
				t.Run(fmt.Sprintf("Native body %q", clause), func(t *testing.T) {
					requireStdRows2130(t, db, query2130+clause)
				})
			}
		})
	}
}

// requireRows2130 fails the test unless the query returns wantRows2130.
func requireRows2130(t *testing.T, conn driver.Conn, query string) {
	t.Helper()

	var got []row2130
	require.NoError(t, conn.Select(context.Background(), &got, query))
	assert.Equal(t, wantRows2130, got)
}

// requireStdRows2130 fails the test unless the query returns wantRows2130.
func requireStdRows2130(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "version"}, columns)
	var got []row2130
	for rows.Next() {
		var r row2130
		require.NoError(t, rows.Scan(&r.ID, &r.Version))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, wantRows2130, got)
}
