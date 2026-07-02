package issues

import (
	"context"
	"crypto/tls"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// queryRowFunc abstracts single-row parameter binding + scan so the same
// assertions can run against both the native (Open) and std (OpenDB) APIs.
type queryRowFunc func(dest any, query string, args ...any) error

// Test1862_FloatBinding is a regression test for issue #1862: binding a
// float64/float32 parameter used to fall through to fmt.Sprint, so an
// integer-valued float like 1.0 rendered as the bare literal "1".
//
// ClickHouse then inferred an integer type and a later typed scan into a *float64 failed
// with "converting UInt8 to *float64 is unsupported". Non-finite values were
// broken too, since Go prints "NaN"/"+Inf" while ClickHouse only parses the
// lowercase nan/inf spellings.
//
// The float now renders as cast(<value>, 'Float32'|'Float64'), so this test
// binds float scalars, arrays, and maps and scans them back, exercising the
// fix end-to-end across both protocols (native TCP and HTTP) and both APIs.
func Test1862_FloatBinding(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}

	ctx := context.Background()

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		protocol := protocol

		t.Run("native/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, tlsConfig, nil)
			require.NoError(t, err)
			runFloatBindingAssertions(t, func(dest any, query string, args ...any) error {
				return conn.QueryRow(ctx, query, args...).Scan(dest)
			})
		})

		t.Run("std/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_std_tests.GetOpenDBConnection("issues", protocol, nil, tlsConfig, nil)
			require.NoError(t, err)
			defer conn.Close()
			runFloatBindingAssertions(t, func(dest any, query string, args ...any) error {
				return conn.QueryRowContext(ctx, query, args...).Scan(dest)
			})
		})
	}
}

func runFloatBindingAssertions(t *testing.T, queryRow queryRowFunc) {
	t.Run("float64", func(t *testing.T) {
		cases := []struct {
			name  string
			param float64
		}{
			{"integer-valued", 1.0},
			{"fractional", 1.5},
			{"negative", -2.0},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				var got float64
				require.NoError(t, queryRow(&got, "SELECT ?", c.param))
				assert.Equal(t, c.param, got, "bound %v", c.param)
			})
		}
	})

	t.Run("float32", func(t *testing.T) {
		cases := []struct {
			name  string
			param float32
		}{
			{"integer-valued", 1.0},
			{"fractional", 2.5},
			{"negative", -2.0},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				var got float32
				require.NoError(t, queryRow(&got, "SELECT ?", c.param))
				assert.Equal(t, c.param, got, "bound %v", c.param)
			})
		}
	})

	t.Run("non-finite", func(t *testing.T) {
		var got float64

		require.NoError(t, queryRow(&got, "SELECT ?", math.Inf(1)))
		assert.True(t, math.IsInf(got, 1), "expected +Inf, got %v", got)

		require.NoError(t, queryRow(&got, "SELECT ?", math.Inf(-1)))
		assert.True(t, math.IsInf(got, -1), "expected -Inf, got %v", got)

		require.NoError(t, queryRow(&got, "SELECT ?", math.NaN()))
		assert.True(t, math.IsNaN(got), "expected NaN, got %v", got)
	})

	t.Run("array of float64", func(t *testing.T) {
		var got []float64
		require.NoError(t, queryRow(&got, "SELECT ?", []float64{1.0, 2.5, -3.0}))
		assert.Equal(t, []float64{1.0, 2.5, -3.0}, got)
	})

	t.Run("map of float64", func(t *testing.T) {
		var got map[string]float64
		require.NoError(t, queryRow(&got, "SELECT ?", map[string]float64{"a": 1.0, "b": 2.5}))
		assert.Equal(t, map[string]float64{"a": 1.0, "b": 2.5}, got)
	})
}
