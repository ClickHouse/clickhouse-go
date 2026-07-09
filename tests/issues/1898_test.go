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

// TestIssue1898_MapQueryParameter covers the fix for #1898: a Go map sent as
// a `Map` query parameter must arrive as `{'k':v}` — the text format the
// server parses — not the `map('k', v)` SQL syntax used for client-side
// binding. Floats and nested `DateTime` values had the same problem, so they
// are covered here too.
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

		// Quotes and backslashes in string keys must survive the round trip.
		t.Run("Map key escaping", func(t *testing.T) {
			in := map[string]uint8{`a'b\c`: 1}
			var got map[string]uint8
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, UInt8)}",
				clickhouse.Named("m", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// A top-level string is passed to the server as-is (that is also the
		// escape hatch for sending pre-formatted parameter text), and the
		// server then decodes escapes in it: `\'` becomes `'`. Both
		// protocols must agree on the result — before the Field-dump
		// escaping fix, native rejected any value with a backslash before a
		// quote.
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

		// The container syntax must be right at any nesting depth.
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

		// Floats had the same bug: the server rejects the SQL form
		// cast(1.5, 'Float64') and wants the plain number.
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

		// time.Time as a plain Named parameter: sent raw at the top level,
		// quoted when nested inside a map or array.
		t.Run("DateTime round-trip", func(t *testing.T) {
			in := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Pointers to strings and times must get the same top-level raw
		// treatment as their plain counterparts.
		t.Run("pointer round-trips", func(t *testing.T) {
			s := "hello"
			var gotStr string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {s:String}",
				clickhouse.Named("s", &s),
			).Scan(&gotStr))
			require.Equal(t, s, gotStr)

			d := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			var gotTime time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", &d),
			).Scan(&gotTime))
			require.Equal(t, d, gotTime)
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

		// A sub-second time.Time keeps its fraction, so DateTime64
		// parameters don't silently lose precision — top-level and nested.
		t.Run("DateTime64 keeps sub-second precision", func(t *testing.T) {
			in := time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC)

			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime64(9, 'UTC')}",
				clickhouse.Named("d", in),
			).Scan(&got))
			require.Equal(t, in, got)

			var gotMap map[string]time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, DateTime64(9, 'UTC'))}",
				clickhouse.Named("m", map[string]time.Time{"a": in}),
			).Scan(&gotMap))
			require.Equal(t, map[string]time.Time{"a": in}, gotMap)
		})

		// A time.Time carrying a non-UTC zone must keep its instant. Times
		// are sent as epoch (parameter text has no timezone syntax), so the
		// parameter's declared zone changes only the rendering, never the
		// instant. Before the fix the wall-clock text was re-interpreted in
		// the parameter's zone, shifting the instant by the zone offset.
		t.Run("non-UTC time keeps its instant", func(t *testing.T) {
			tokyo := time.FixedZone("Asia/Tokyo", 9*3600)
			in := time.Date(2020, 1, 2, 12, 0, 0, 0, tokyo) // == 03:00:00 UTC

			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", in),
			).Scan(&got))
			require.True(t, got.Equal(in), "want instant %s, got %s", in.UTC(), got.UTC())

			var gotMap map[string]time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, DateTime('UTC'))}",
				clickhouse.Named("m", map[string]time.Time{"a": in}),
			).Scan(&gotMap))
			require.True(t, gotMap["a"].Equal(in), "want instant %s, got %s", in.UTC(), gotMap["a"].UTC())
		})

		// A nil parameter must arrive as SQL NULL. Before the fix it was
		// sent as the text `NULL`, which a Nullable(String) parameter
		// silently read as the string "NULL" and other types rejected.
		t.Run("nil round-trips as NULL", func(t *testing.T) {
			var gotStr *string
			var isNull uint8
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {s:Nullable(String)}, isNull({s:Nullable(String)})",
				clickhouse.Named("s", (*string)(nil)),
			).Scan(&gotStr, &isNull))
			require.Nil(t, gotStr)
			require.Equal(t, uint8(1), isNull)

			var gotInt *int32
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {i:Nullable(Int32)}",
				clickhouse.Named("i", nil),
			).Scan(&gotInt))
			require.Nil(t, gotInt)

			// nils nested inside a composite must keep working too
			s := "x"
			in := []*string{&s, nil}
			var gotArr []*string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {a:Array(Nullable(String))}",
				clickhouse.Named("a", in),
			).Scan(&gotArr))
			require.Equal(t, in, gotArr)
		})

		// A sub-second time.Time sent to a plain DateTime parameter fails
		// loudly instead of silently dropping the fraction. DateNamed is
		// the way to pick the scale explicitly.
		t.Run("sub-second time into DateTime errors", func(t *testing.T) {
			in := time.Date(2020, 1, 2, 3, 4, 5, 123000000, time.UTC)
			var got time.Time
			require.Error(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", in),
			).Scan(&got))
		})
	})
}
