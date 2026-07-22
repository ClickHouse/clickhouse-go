package issues

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1917_BigIntBindParameter covers the fix for #1917: a *big.Int bind
// parameter (the Go type behind Int128/UInt128/Int256/UInt256) used to be
// spliced into the query as a quoted string literal, so the server saw a String
// (`toTypeName(?)` returned `String`) instead of a number. It is now emitted as
// a wide-integer conversion, keeping both the value and an integer type.
//
// Native and HTTP share the same bind code path, so every case runs on both.
func TestIssue1917_BigIntBindParameter(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		bigStr := func(s string) *big.Int {
			v, ok := new(big.Int).SetString(s, 10)
			require.True(t, ok)
			return v
		}

		values := []*big.Int{
			big.NewInt(42),
			big.NewInt(-42),
			bigStr("170141183460469231731687303715884105727"),                                        // Int128 max
			bigStr("-170141183460469231731687303715884105728"),                                       // Int128 min
			bigStr("340282366920938463463374607431768211455"),                                        // UInt128 max
			bigStr("57896044618658097711785492504343953926634992332820282019728792003956564819967"),  // Int256 max
			bigStr("115792089237316195423570985008687907853269984665640564039457584007913129639935"), // UInt256 max
		}

		// A big.Int bind parameter must bind as a wide integer, not a String
		// (the bug) and not a Float64 (what a bare >64-bit decimal literal would
		// infer, losing precision).
		t.Run("bound parameter binds as a wide integer", func(t *testing.T) {
			wideInts := []string{"Int128", "UInt128", "Int256", "UInt256"}
			for _, v := range values {
				var typeName string
				require.NoError(t, conn.QueryRow(ctx, "SELECT toTypeName(?)", v).Scan(&typeName))
				require.Contains(t, wideInts, typeName, "value %s bound as %s", v, typeName)
			}
		})

		// The bound value must round-trip exactly. A bare decimal literal wider
		// than 64 bits would be parsed as Float64 and lose precision, so the
		// wide-integer conversion is what keeps this exact.
		t.Run("value round-trips exactly", func(t *testing.T) {
			for _, v := range values {
				var got big.Int
				require.NoError(t, conn.QueryRow(ctx, "SELECT ?", v).Scan(&got))
				require.Zero(t, got.Cmp(v), "want %s, got %s", v, got.String())
			}
		})

		// The primary use case: matching an Int128 column. The old quoted-string
		// form coerced to Int128 and worked here; the new form must keep working
		// (and, unlike a bare literal, still match the exact row).
		t.Run("matches an Int128 column", func(t *testing.T) {
			const ddl = "CREATE TABLE test_1917 (`id` Int128) ENGINE = Memory"
			require.NoError(t, conn.Exec(ctx, ddl))
			t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1917") })

			// Insert through the native binary path (unaffected by #1917) so
			// the row holds the exact value, then match it via the bind path.
			id := bigStr("170141183460469231731687303715884105727")
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1917")
			require.NoError(t, err)
			require.NoError(t, batch.Append(id))
			require.NoError(t, batch.Send())

			var count uint64
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT count() FROM test_1917 WHERE id = ?", id).Scan(&count))
			require.Equal(t, uint64(1), count)
		})

		// Server-side query parameters ({name:Type}) declare the type, so the
		// value is sent as a bare decimal and parsed exactly.
		t.Run("server-side query parameter", func(t *testing.T) {
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
				t.Skip("server-side query parameters require ClickHouse 22.8+")
			}
			v := bigStr("170141183460469231731687303715884105727")
			var got big.Int
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {val:Int128}", clickhouse.Named("val", v)).Scan(&got))
			require.Zero(t, got.Cmp(v), "want %s, got %s", v, got.String())
		})
	})
}
