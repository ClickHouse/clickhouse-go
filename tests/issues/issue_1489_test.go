package issues

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// Test1489_QueryParameterDetection verifies end-to-end across native and HTTP
// protocols (and both the native API and database/sql) that `{...:...}` inside
// string literals, quoted identifiers, or SQL comments does not force
// server-side query-parameter mode (#1489, #1828), while genuine `{name:Type}`
// placeholders next to literals or comments still do.
func Test1489_QueryParameterDetection(t *testing.T) {
	ctx := context.Background()
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}

	type queryRowFn func(query string, args ...any) interface{ Scan(dest ...any) error }

	runCases := func(
		t *testing.T,
		queryRow queryRowFn,
		supportsJSONMergePatch bool,
		supportsServerParams bool,
	) {
		t.Helper()

		t.Run("braces and colon in literals with ?", func(t *testing.T) {
			var one, two, three string
			require.NoError(t, queryRow("SELECT '{' AS one, ':' AS two, '}' AS three WHERE ? = 1", 1).Scan(&one, &two, &three))
			assert.Equal(t, "{", one)
			assert.Equal(t, ":", two)
			assert.Equal(t, "}", three)

			var combined string
			require.NoError(t, queryRow("SELECT concat('{', ':', '}') WHERE ? = 1", 1).Scan(&combined))
			assert.Equal(t, "{:}", combined)

			var count uint64
			require.NoError(t, queryRow(`SELECT count() FROM numbers(?) WHERE '{"a":1}' != ''`, uint64(5)).Scan(&count))
			assert.Equal(t, uint64(5), count)
		})

		t.Run("jsonMergePatch on one line with ?", func(t *testing.T) {
			if !supportsJSONMergePatch {
				t.Skip("jsonMergePatch requires ClickHouse 23.10+")
			}
			var merged string
			require.NoError(t, queryRow(
				`SELECT jsonMergePatch('{"test":' || toString(number * 10) || '}') FROM numbers(3) WHERE number = ?`,
				uint64(2),
			).Scan(&merged))
			assert.Equal(t, `{"test":20}`, merged)
		})

		t.Run("placeholder pattern in comments (#1828)", func(t *testing.T) {
			limitArg := clickhouse.Named("limit", uint32(5))
			for _, tc := range []struct {
				name  string
				query string
				arg   any
			}{
				{
					name:  "line comment matching @name",
					query: "-- server-side: {limit:UInt32}\nSELECT count() FROM numbers(@limit)",
					arg:   limitArg,
				},
				{
					name:  "line comment other name with @name",
					query: "-- see {name:Type}\nSELECT count() FROM numbers(@limit)",
					arg:   limitArg,
				},
				{
					name:  "block comment with @name",
					query: "/* {limit:UInt32} */ SELECT count() FROM numbers(@limit)",
					arg:   limitArg,
				},
				{
					name:  "line comment with ?",
					query: "-- see {name:Type}\nSELECT count() FROM numbers(?)",
					arg:   uint64(5),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var count uint64
					require.NoError(t, queryRow(tc.query, tc.arg).Scan(&count))
					assert.Equal(t, uint64(5), count)
				})
			}
		})

		t.Run("quoted identifier with ?", func(t *testing.T) {
			var got uint64
			require.NoError(t, queryRow(
				"SELECT `{id:UInt32}` FROM (SELECT toUInt64(42) AS `{id:UInt32}`) WHERE ? = 1",
				1,
			).Scan(&got))
			assert.Equal(t, uint64(42), got)
		})

		t.Run("real server-side parameter beside JSON literal", func(t *testing.T) {
			if !supportsServerParams {
				t.Skip("server-side query parameters require ClickHouse 22.8+")
			}
			var id uint32
			var jsonText string
			require.NoError(t, queryRow(
				`SELECT {id:UInt32}, '{"a":1}'`,
				clickhouse.Named("id", uint32(7)),
			).Scan(&id, &jsonText))
			assert.Equal(t, uint32(7), id)
			assert.Equal(t, `{"a":1}`, jsonText)
		})
	}

	t.Run("native", func(t *testing.T) {
		for _, protocol := range protocols {
			t.Run(protocol.String(), func(t *testing.T) {
				conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				supportsJSONMergePatch := clickhouse_tests.CheckMinServerServerVersion(conn, 23, 10, 0)
				supportsServerParams := clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0)
				runCases(t, func(query string, args ...any) interface{ Scan(dest ...any) error } {
					return conn.QueryRow(ctx, query, args...)
				}, supportsJSONMergePatch, supportsServerParams)
			})
		}
	})

	t.Run("database/sql", func(t *testing.T) {
		useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)

		for _, protocol := range protocols {
			t.Run(protocol.String(), func(t *testing.T) {
				db, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })

				supportsJSONMergePatch := clickhouse_std_tests.CheckMinServerVersion(db, 23, 10, 0)
				supportsServerParams := clickhouse_std_tests.CheckMinServerVersion(db, 22, 8, 0)
				runCases(t, func(query string, args ...any) interface{ Scan(dest ...any) error } {
					return db.QueryRowContext(ctx, query, args...)
				}, supportsJSONMergePatch, supportsServerParams)
			})
		}
	})
}
