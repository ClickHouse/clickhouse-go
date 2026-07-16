package std

import (
	"testing"

	"math/big"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TestStd1917BigIntBindParameter covers the database/sql surface of the #1917
// fix. The std path reaches the same client-side bind rewrite as the native
// driver (rebind -> bind -> format), so a big.Int bind parameter — the Go type
// behind Int128/UInt128/Int256/UInt256 — must be sent as a wide integer, not
// the quoted string that made the server report toTypeName(?) as String.
func TestStd1917BigIntBindParameter(t *testing.T) {
	bigStr := func(s string) *big.Int {
		v, ok := new(big.Int).SetString(s, 10)
		require.True(t, ok)
		return v
	}

	values := []*big.Int{
		big.NewInt(42),
		big.NewInt(-42),
		bigStr("170141183460469231731687303715884105727"),                                        // Int128 max
		bigStr("115792089237316195423570985008687907853269984665640564039457584007913129639935"), // UInt256 max
	}
	wideInts := []string{"Int128", "UInt128", "Int256", "UInt256"}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := GetStdOpenDBConnection(protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			for _, v := range values {
				// The parameter binds as a wide integer, never a String (the
				// bug) and never a bare >64-bit literal (which infers Float64).
				var typeName string
				require.NoError(t, conn.QueryRow("SELECT toTypeName(?)", v).Scan(&typeName))
				require.Contains(t, wideInts, typeName, "value %s bound as %s", v, typeName)

				// The value survives the round-trip exactly.
				var got string
				require.NoError(t, conn.QueryRow("SELECT toString(?)", v).Scan(&got))
				require.Equal(t, v.String(), got)
			}
		})
	}
}
