package tests

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleBFloat16(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16 (
			  Col1 BFloat16,
			  Col2 Nullable(BFloat16)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16")
		require.NoError(t, err)
		require.NoError(t, batch.Append(float32(33.125), sql.NullFloat64{
			Float64: 34.25,
			Valid:   true,
		}))
		require.Equal(t, 1, batch.Rows())
		assert.NoError(t, batch.Send())

		// BFloat16 may have precision loss, so check with tolerance
		// BFloat16 has 7-bit for mantissa compared to 23-bit mantissa for Float32.
		// which makes it loose 3-4 digits of precision.
		relativeError := 0.004 // 0.4%
		var (
			col1 float32
			col2 sql.NullFloat64
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bfloat16").Scan(&col1, &col2))
		// BFloat16 has limited precision, so use tolerance of 0.04% of the original number
		require.InDelta(t, float32(33.125), col1, (33.125)*relativeError)
		require.True(t, col2.Valid)
		require.InDelta(t, 34.25, col2.Float64, (34.25)*relativeError)
	})
}

type customBFloat16 float32

func (f *customBFloat16) Scan(src any) error {
	if t, ok := src.(float32); ok {
		*f = customBFloat16(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customBFloat16", src)
}

func TestCustomBFloat16(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16_custom (
			  Col1 BFloat16
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16_custom")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16_custom")
		require.NoError(t, err)
		require.NoError(t, batch.Append(float32(123.456)))
		require.NoError(t, batch.Send())

		var col1 customBFloat16
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bfloat16_custom").Scan(&col1))
		require.InDelta(t, float32(123.456), float32(col1), 1.0)
	})
}

func TestBFloat16Flush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "Flush")
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16_flush (
			  Col1 BFloat16
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16_flush")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16_flush")
		require.NoError(t, err)
		vals := [1000]float32{}
		for i := range 1000 {
			vals[i] = rand.Float32() * 1000
			require.NoError(t, batch.Append(vals[i]))
			if (i+1)%100 == 0 {
				require.NoError(t, batch.Flush())
			}
		}
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT * FROM test_bfloat16_flush")
		require.NoError(t, err)

		// BFloat16 may have precision loss, so check with tolerance
		// BFloat16 has 7-bit for mantissa compared to 23-bit mantissa for Float32.
		// which makes it loose 3-4 digits of precision.
		relativeError := 0.004 // 0.4%

		i := 0
		for rows.Next() {
			var col1 float32
			require.NoError(t, rows.Scan(&col1))

			maxDelta := float64(vals[i]) * relativeError
			require.InDelta(t, vals[i], col1, float64(maxDelta)) // BFloat16 precision
			i++
		}
		require.Equal(t, 1000, i)
	})
}

func TestBFloat16EdgeCases(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16_edge (
			  Col1 BFloat16
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16_edge")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		testCases := []struct {
			name  string
			value float32
		}{
			{"zero", 0.0},
			{"negative_zero", float32(math.Copysign(0, -1))},
			{"positive", 3.14},
			{"negative", -3.14},
			{"small_positive", 0.00001},
			{"small_negative", -0.00001},
			{"large_positive", 10000.0},
			{"large_negative", -10000.0},
			{"positive_infinity", float32(math.Inf(1))},
			{"negative_infinity", float32(math.Inf(-1))},
			{"nan", float32(math.NaN())},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				batch, _ := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16_edge")
				batch.Append(tc.value)
				batch.Send()
				var result float32
				conn.QueryRow(ctx, "SELECT * FROM test_bfloat16_edge ORDER BY Col1 DESC LIMIT 1").Scan(&result)

				if math.IsNaN(float64(tc.value)) {
					require.True(t, math.IsNaN(float64(result)), "expected NaN")
				} else if math.IsInf(float64(tc.value), 0) {
					require.True(t, math.IsInf(float64(result), int(math.Copysign(1, float64(tc.value)))), "expected infinity with same sign")
				} else {
					require.InDelta(t, tc.value, result, math.Max(0.01*math.Abs(float64(tc.value)), 0.001))
				}

				conn.Exec(ctx, "TRUNCATE TABLE test_bfloat16_edge")
			})
		}
	})
}

func TestBFloat16Precision(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16_precision (
			  Col1 BFloat16
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16_precision")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		// Test that BFloat16 has limited precision (~7 bits mantissa)
		testCases := []struct {
			input    float32
			expected float32
			delta    float64
		}{
			{1.0, 1.0, 0.0},
			{2.0, 2.0, 0.0},
			{3.14159, 3.140625, 0.002},
			{100.5, 100.5, 0.5},
		}

		for _, tc := range testCases {
			batch, _ := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16_precision")
			batch.Append(tc.input)
			batch.Send()
			var result float32
			conn.QueryRow(ctx, "SELECT * FROM test_bfloat16_precision ORDER BY Col1 DESC LIMIT 1").Scan(&result)
			require.InDelta(t, tc.expected, result, tc.delta, "input=%v", tc.input)
			conn.Exec(ctx, "TRUNCATE TABLE test_bfloat16_precision")
		}
	})
}

func TestBFloat16Array(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 24, 11, 0) {
			t.Skip(fmt.Errorf("BFloat16 requires ClickHouse 24.11+"))
			return
		}
		const ddl = `
		CREATE TABLE test_bfloat16_array (
			  Col1 Array(BFloat16)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bfloat16_array")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bfloat16_array")
		require.NoError(t, err)

		expected := []float32{1.5, 2.5, 3.5, 4.5}
		require.NoError(t, batch.Append(expected))
		require.NoError(t, batch.Send())

		var result []float32
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bfloat16_array").Scan(&result))
		require.Equal(t, len(expected), len(result))
		for i := range expected {
			require.InDelta(t, expected[i], result[i], 0.01)
		}
	})
}

func BenchmarkBFloat16(b *testing.B) {
	conn, err := GetNativeConnectionTCP(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}
	if !CheckMinServerServerVersion(conn, 24, 11, 0) {
		b.Skip("BFloat16 requires ClickHouse 24.11+")
		return
	}
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_bfloat16")

	conn.Exec(ctx, `CREATE TABLE benchmark_bfloat16 (Col1 BFloat16) ENGINE = Null`)

	const rowsInBlock = 10_000_000

	for b.Loop() {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_bfloat16 VALUES")
		if err != nil {
			b.Fatal(err)
		}
		for range rowsInBlock {
			if err := batch.Append(float32(122.112)); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}
