package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1891_ArrayBoolQueryParameter covers the fix for #1891: bools in
// server-side query parameters must be sent as `true`/`false`, not `1`/`0`.
// The server parses parameter values as text, and for types like `Array(Bool)`
// it rejects `1`/`0` with a CANNOT_READ_ARRAY_FROM_TEXT error (code 130).
//
// Native and HTTP share the same query-parameter code path, so every case
// runs on both protocols.
func TestIssue1891_ArrayBoolQueryParameter(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		defer conn.Close()

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			t.Skip("server-side query parameters require ClickHouse 22.8+")
		}

		// The query from the original bug report: an Array(Bool) parameter
		// used inside hasAll.
		t.Run("Array(Bool) in hasAll", func(t *testing.T) {
			var ok bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT hasAll([true, false], {vals:Array(Bool)})",
				clickhouse.Named("vals", []bool{true, false}),
			).Scan(&ok))
			require.True(t, ok)
		})

		// Send arrays with different value mixes and read them back. The
		// all-false array matters: it shows `false` becomes `false`, not `0`.
		t.Run("Array(Bool) round-trip", func(t *testing.T) {
			for _, in := range [][]bool{
				{true, false},
				{false, false},
				{true, true},
			} {
				var got []bool
				require.NoError(t, conn.QueryRow(ctx,
					"SELECT {vals:Array(Bool)}",
					clickhouse.Named("vals", in),
				).Scan(&got))
				require.Equal(t, in, got)
			}
		})

		// Nested arrays: bools two levels deep must also be formatted as text.
		t.Run("Array(Array(Bool)) round-trip", func(t *testing.T) {
			in := [][]bool{{true, false}, {false, true}}
			var got [][]bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {vals:Array(Array(Bool))}",
				clickhouse.Named("vals", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Nullable bools inside an array: real values become true/false,
		// nil stays NULL.
		t.Run("Array(Nullable(Bool)) round-trip", func(t *testing.T) {
			tru, fls := true, false
			in := []*bool{&tru, nil, &fls}
			var got []*bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {vals:Array(Nullable(Bool))}",
				clickhouse.Named("vals", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// A plain Bool parameter still works: the server accepts `true`/`false`
		// for a top-level Bool just as it accepted `1`/`0`, so nothing breaks.
		t.Run("scalar Bool still round-trips", func(t *testing.T) {
			for _, in := range []bool{true, false} {
				var got bool
				require.NoError(t, conn.QueryRow(ctx,
					"SELECT {b:Bool}",
					clickhouse.Named("b", in),
				).Scan(&got))
				require.Equal(t, in, got)
			}
		})
	})
}
