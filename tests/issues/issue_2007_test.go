package issues

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue2007LowCardinalityFloats(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse.Settings{
				"allow_suspicious_low_cardinality_types": 1,
			}, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, conn.Close()) })
			ctx := context.Background()
			const table = "issue_2007_lowcardinality_floats"
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
			require.NoError(t, conn.Exec(ctx, `CREATE TABLE `+table+` (
				id UInt8,
				f32 LowCardinality(Float32), f64 LowCardinality(Float64),
				n32 LowCardinality(Nullable(Float32)), n64 LowCardinality(Nullable(Float64))
			) ENGINE = Memory`))
			t.Cleanup(func() { assert.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)) })
			values := []float64{0, math.Copysign(0, -1), math.NaN(), math.NaN(), math.Inf(1), math.Inf(-1), 1.5}
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
			require.NoError(t, err)
			for i, value := range values {
				require.NoError(t, batch.Append(uint8(i), float32(value), value, float32(value), value))
			}
			require.NoError(t, batch.Append(uint8(len(values)), float32(0), float64(0), nil, nil))
			require.NoError(t, batch.Send())
			rows, err := conn.Query(ctx, "SELECT f32, f64, n32, n64 FROM "+table+" ORDER BY id")
			require.NoError(t, err)
			defer func() { assert.NoError(t, rows.Close()) }()
			for _, want := range values {
				require.True(t, rows.Next())
				var f32 float32
				var f64 float64
				var n32 *float32
				var n64 *float64
				require.NoError(t, rows.Scan(&f32, &f64, &n32, &n64))
				require.NotNil(t, n32)
				require.NotNil(t, n64)
				if math.IsNaN(want) {
					assert.True(t, math.IsNaN(float64(f32)))
					assert.True(t, math.IsNaN(f64))
					assert.True(t, math.IsNaN(float64(*n32)))
					assert.True(t, math.IsNaN(*n64))
				} else if want == 0 {
					// ClickHouse can merge signed zeros in its LowCardinality
					// dictionary. Exact wire bits are checked by the column unit tests.
					assert.Zero(t, f32)
					assert.Zero(t, f64)
					assert.Zero(t, *n32)
					assert.Zero(t, *n64)
				} else {
					assert.Equal(t, math.Float32bits(float32(want)), math.Float32bits(f32))
					assert.Equal(t, math.Float64bits(want), math.Float64bits(f64))
					assert.Equal(t, math.Float32bits(float32(want)), math.Float32bits(*n32))
					assert.Equal(t, math.Float64bits(want), math.Float64bits(*n64))
				}
			}
			require.True(t, rows.Next())
			var f32 float32
			var f64 float64
			var n32 *float32
			var n64 *float64
			require.NoError(t, rows.Scan(&f32, &f64, &n32, &n64))
			assert.Nil(t, n32)
			assert.Nil(t, n64)
			assert.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
