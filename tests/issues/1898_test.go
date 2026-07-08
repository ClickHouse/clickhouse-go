package issues

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1898_MapQueryParameter covers the fix for #1898: `Map`-typed
// server-side query parameters must be sent in the `{'k':v}` text format the
// server's parameter parser expects, not the `map('k', v)` SQL-function
// syntax used for client-side binding. The same formatting split applies to
// floats (plain literal, not `cast(...)`) and nested `DateTime` values
// (quoted text, not `toDateTime(...)`).
//
// Native and HTTP share the same query-parameter code path, so every case
// runs on both protocols.
func TestIssue1898_MapQueryParameter(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		defer conn.Close()

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			t.Skip("server-side query parameters require ClickHouse 22.8+")
		}

		// The repro from the bug report.
		t.Run("Map(String, Bool) round-trip", func(t *testing.T) {
			in := map[string]bool{"a": true, "b": false}
			var got map[string]bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, Bool)}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// The bug affected every Map value type, not just Bool.
		t.Run("Map(String, String) round-trip", func(t *testing.T) {
			in := map[string]string{"a": "x", "b": "y"}
			var got map[string]string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, String)}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Quotes and backslashes in string keys must survive the text format.
		t.Run("Map key escaping", func(t *testing.T) {
			in := map[string]uint8{`a'b\c`: 1}
			var got map[string]uint8
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, UInt8)}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Top-level strings are passed through raw (they double as the
		// escape hatch for pre-formatted parameter text), so the server
		// applies its escaped-text semantics: `\'` decodes to `'`. Both
		// protocols must agree — before the native Field-dump escaping fix,
		// native rejected any value with a backslash before a quote.
		t.Run("String passes through raw on both protocols", func(t *testing.T) {
			var got string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {s:String}",
				clickhouse.Named("s", `a'b\'c`),
			).Scan(&got))
			require.Equal(t, `a'b'c`, got)
		})

		t.Run("empty map", func(t *testing.T) {
			var got map[string]string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, String)}",
				clickhouse.Named("m", map[string]string{}),
			).Scan(&got))
			require.Empty(t, got)
		})

		// Nested composites: the container syntax must be right at any depth.
		t.Run("Map(String, Map(String, Bool)) round-trip", func(t *testing.T) {
			in := map[string]map[string]bool{"a": {"x": true, "y": false}}
			var got map[string]map[string]bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, Map(String, Bool))}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		t.Run("Array(Map(String, String)) round-trip", func(t *testing.T) {
			in := []map[string]string{{"a": "x"}, {"b": "y"}}
			var got []map[string]string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Array(Map(String, String))}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		t.Run("Map(String, Array(Bool)) round-trip", func(t *testing.T) {
			in := map[string][]bool{"a": {true, false}}
			var got map[string][]bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, Array(Bool))}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Floats hit the same issue class: the SQL-literal form
		// cast(1.5, 'Float64') is rejected by the parameter text parser.
		t.Run("Float64 round-trip", func(t *testing.T) {
			for _, in := range []float64{1.5, -3.25, 0, math.Inf(1), math.Inf(-1)} {
				var got float64
				require.NoError(t, conn.QueryRow(ctx,
					"SELECT {f:Float64}",
					clickhouse.Named("f", in),
				).Scan(&got))
				require.Equal(t, in, got)
			}

			var got float64
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {f:Float64}",
				clickhouse.Named("f", math.NaN()),
			).Scan(&got))
			require.True(t, math.IsNaN(got))
		})

		t.Run("Map(String, Float64) round-trip", func(t *testing.T) {
			in := map[string]float64{"a": 1.5}
			var got map[string]float64
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, Float64)}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// time.Time as a plain Named parameter: raw text at the top level,
		// quoted text when nested inside a composite type.
		t.Run("DateTime round-trip", func(t *testing.T) {
			in := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		t.Run("Map(String, DateTime) round-trip", func(t *testing.T) {
			in := map[string]time.Time{"a": time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)}
			var got map[string]time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, DateTime('UTC'))}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})
	})
}
