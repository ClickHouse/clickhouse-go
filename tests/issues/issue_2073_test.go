package issues

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue2073_TupleElementNameWithSpace(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()

			tableName := fmt.Sprintf("test_2073_%v", protocol)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (Col1 Tuple(`a b` Int64, `price (usd)` Float64)) Engine MergeTree() ORDER BY tuple()", tableName)))
			t.Cleanup(func() { _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })

			mapData := map[string]any{"a b": int64(1), "price (usd)": float64(1.5)}
			batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
			require.NoError(t, err)
			require.NoError(t, batch.Append(mapData))
			require.NoError(t, batch.Send())

			var gotMap map[string]any
			require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT Col1 FROM %s", tableName)).Scan(&gotMap))
			assert.Equal(t, mapData, gotMap)
		})
	}
}

func TestIssue2073_QuotedNamesInCompositeTypes(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			settings := clickhouse_tests.TestClientDefaultSettings(testEnv)
			settings["flatten_nested"] = 0
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, settings, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, conn.Close()) })
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 4, 0) {
				t.Skip("unsupported clickhouse version for Variant type")
			}
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				"allow_experimental_variant_type": true,
			}))

			variant := clickhouse.NewVariantWithType(uint64(8), "UInt64")
			nested := []map[string]any{{"z)": int8(7), "value": "ok"}}
			for _, tc := range []struct {
				name     string
				query    string
				dest     any
				expected any
			}{
				{
					name:     "Variant retains the alternative after a quoted parenthesis",
					query:    "SELECT CAST(toUInt64(8) AS Variant(Tuple(`z)` Int8), UInt64))",
					dest:     new(clickhouse.Variant),
					expected: &variant,
				},
				{
					name:     "Nested retains both quoted and plain fields",
					query:    "SELECT CAST([(toInt8(7), 'ok')] AS Nested(`z)` Int8, value String))",
					dest:     new([]map[string]any),
					expected: &nested,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.NoError(t, conn.QueryRow(ctx, tc.query).Scan(tc.dest))
					assert.Equal(t, tc.expected, tc.dest)
				})
			}
		})
	}
}

func TestIssue2073_CompositeRoundTrips(t *testing.T) {
	type taggedTuple struct {
		Number int8   `ch:"a b"`
		Text   string `ch:"z)"`
	}
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	nullableLabel := "x)y"

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			settings := clickhouse_tests.TestClientDefaultSettings(testEnv)
			settings["flatten_nested"] = 0
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, settings, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, conn.Close()) })
			options := clickhouse_tests.ClientOptionsFromEnv(testEnv, settings, protocol == clickhouse.HTTP)
			stdConn := clickhouse.OpenDB(&options)
			t.Cleanup(func() { assert.NoError(t, stdConn.Close()) })

			for i, tc := range []struct {
				name         string
				columnType   string
				value        any
				newDest      func() any
				newStdDest   func() any
				stdWant      any
				needsVariant bool
			}{
				{
					name:       "Nested quoted fields",
					columnType: "Nested(`a, b` Int8, `z)` String)",
					value:      []map[string]any{{"a, b": int8(7), "z)": "nested"}},
					newDest:    func() any { return new([]map[string]any) },
				},
				{
					name:       "recursive Nested quoted fields",
					columnType: "Nested(children Nested(`a b` Int8, `z)` String), `x, y` UInt8)",
					value:      []map[string]any{{"children": []map[string]any{{"a b": int8(7), "z)": "child"}}, "x, y": uint8(1)}},
					newDest:    func() any { return new([]map[string]any) },
				},
				{
					name:         "Variant Tuple alternative",
					columnType:   "Variant(Tuple(`a, b` Int8, `z)` String), UInt64)",
					value:        clickhouse.NewVariant(map[string]any{"a, b": int8(7), "z)": "variant"}),
					newDest:      func() any { return new(clickhouse.Variant) },
					needsVariant: true,
				},
				{
					name:       "Map of quoted Tuple with Nullable Enum",
					columnType: "Map(String, Tuple(`a, b` Nullable(Enum8('x)y' = 1, 'z' = 2)), `z)` String))",
					value:      map[string]map[string]any{"key": {"a, b": &nullableLabel, "z)": "map"}},
					newDest:    func() any { return new(map[string]map[string]any) },
				},
				{
					name:       "Array of quoted Tuple",
					columnType: "Array(Tuple(`a, b` Int8, `z)` String))",
					value:      []map[string]any{{"a, b": int8(7), "z)": "array"}},
					newDest:    func() any { return new([]map[string]any) },
				},
				{
					name:       "escaped Tuple identifiers",
					columnType: "Tuple(`back\\`tick` Int8, `slash\\\\` String, `line\\nbreak` String)",
					value:      map[string]any{"back`tick": int8(7), "slash\\": "slash", "line\nbreak": "newline"},
					newDest:    func() any { return new(map[string]any) },
				},
				{
					name:       "quoted Tuple struct tags",
					columnType: "Tuple(`a b` Int8, `z)` String)",
					value:      taggedTuple{Number: 7, Text: "struct"},
					newDest:    func() any { return new(taggedTuple) },
					// database/sql returns a named Tuple as a map, not an arbitrary struct.
					newStdDest: func() any { return new(map[string]any) },
					stdWant:    map[string]any{"a b": int8(7), "z)": "struct"},
				},
				{
					name:       "SimpleAggregateFunction of quoted Tuple",
					columnType: "SimpleAggregateFunction(anyLast, Tuple(`a, b` Int8, `z)` String))",
					value:      map[string]any{"a, b": int8(7), "z)": "aggregate"},
					newDest:    func() any { return new(map[string]any) },
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.needsVariant && !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 4, 0) {
						t.Skip("unsupported clickhouse version for Variant type")
					}
					ctx := context.Background()
					if tc.needsVariant {
						ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"allow_experimental_variant_type": true}))
					}
					for _, api := range []string{"native", "std"} {
						t.Run(api, func(t *testing.T) {
							tableName := fmt.Sprintf("test_2073_composite_%v_%s_%d", protocol, api, i)
							require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (value %s) ENGINE=Memory", tableName, tc.columnType)))
							t.Cleanup(func() { assert.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)) })
							insert := "INSERT INTO " + tableName
							if api == "native" {
								batch, err := conn.PrepareBatch(ctx, insert)
								require.NoError(t, err)
								t.Cleanup(func() {
									if !batch.IsSent() {
										assert.NoError(t, batch.Abort())
									}
								})
								require.NoError(t, batch.Append(tc.value))
								require.NoError(t, batch.Send())
							} else {
								tx, err := stdConn.BeginTx(ctx, nil)
								require.NoError(t, err)
								t.Cleanup(func() {
									if err := tx.Rollback(); err != sql.ErrTxDone {
										assert.NoError(t, err)
									}
								})
								stmt, err := tx.PrepareContext(ctx, insert)
								require.NoError(t, err)
								t.Cleanup(func() { assert.NoError(t, stmt.Close()) })
								_, err = stmt.ExecContext(ctx, tc.value)
								require.NoError(t, err)
								require.NoError(t, tx.Commit())
							}
							dest := tc.newDest()
							expected := tc.value
							if api == "std" && tc.newStdDest != nil {
								dest, expected = tc.newStdDest(), tc.stdWant
							}
							query := "SELECT value FROM " + tableName
							if api == "native" {
								require.NoError(t, conn.QueryRow(ctx, query).Scan(dest))
							} else {
								require.NoError(t, stdConn.QueryRowContext(ctx, query).Scan(dest))
							}
							actual := reflect.ValueOf(dest).Elem().Interface()
							if variant, ok := actual.(clickhouse.Variant); ok {
								require.Contains(t, variant.Type(), "Tuple(")
								actual = variant.Any()
								expected = tc.value.(clickhouse.Variant).Any()
							}
							assert.Equal(t, expected, actual)
						})
					}
				})
			}
		})
	}
}
