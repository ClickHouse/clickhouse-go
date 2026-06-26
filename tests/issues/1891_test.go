package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1891_ArrayBoolQueryParameter pins the fix for #1891: a bool passed
// as (part of) a server-side query parameter must be rendered as `true`/`false`,
// not `1`/`0`. ClickHouse's text parser for containers of `Bool` —
// `Array(Bool)`, `Array(Array(Bool))`, `Array(Nullable(Bool))` — rejects `1`/`0`
// with "Cannot read array from text ... CANNOT_READ_ARRAY_FROM_TEXT" (code 130).
//
// The server-side query-parameter path is protocol-agnostic (both Native and
// HTTP funnel through bindQueryOrAppendParameters -> formatValue), so every case
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

		// Original report: an Array(Bool) parameter used inside hasAll.
		t.Run("Array(Bool) in hasAll", func(t *testing.T) {
			var ok bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT hasAll([true, false], {vals:Array(Bool)})",
				clickhouse.Named("vals", []bool{true, false}),
			).Scan(&ok))
			require.True(t, ok)
		})

		// Array(Bool) round-trips for every combination of element values — the
		// all-false case proves `false` renders as `false`, not `0`.
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

		// Nested arrays: the bool-as-text flag must be threaded through every level.
		t.Run("Array(Array(Bool)) round-trip", func(t *testing.T) {
			in := [][]bool{{true, false}, {false, true}}
			var got [][]bool
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {vals:Array(Array(Bool))}",
				clickhouse.Named("vals", in),
			).Scan(&got))
			require.Equal(t, in, got)
		})

		// Nullable bool inside an array: non-null elements must be true/false,
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

		// Contrast: a scalar Bool parameter keeps round-tripping. The server
		// accepts `true`/`false` for a top-level Bool exactly as it accepted the
		// previous `1`/`0`, so the fix does not regress the scalar path.
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
