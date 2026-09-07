package tests

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestQueryParameters(t *testing.T) {
	ctx := context.Background()

	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	client, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	if !CheckMinServerServerVersion(client, 22, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}

	t.Run("with context parameters", func(t *testing.T) {
		chCtx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
			"num":       "42",
			"str":       "hello",
			"array":     "['a', 'b', 'c']",
			"escaped":   `line 1\nline 2\tend`,
			"backslash": `line 1\\nline 2`,
		}))

		var actualNum uint64
		var actualStr string
		var actualArray []string
		var actualEscaped string
		var actualBackslash string
		row := client.QueryRow(chCtx, "SELECT {num:UInt64}, {str:String}, {array:Array(String)}, {escaped:String}, {backslash:String}")
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr, &actualArray, &actualEscaped, &actualBackslash))

		assert.Equal(t, uint64(42), actualNum)
		assert.Equal(t, "hello", actualStr)
		assert.Equal(t, []string{"a", "b", "c"}, actualArray)
		assert.Equal(t, "line 1\nline 2\tend", actualEscaped)
		assert.Equal(t, `line 1\nline 2`, actualBackslash)
	})

	t.Run("with named arguments", func(t *testing.T) {
		var actualNum uint64
		var actualStr string
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			clickhouse.Named("num", "42"),
			clickhouse.Named("str", "hello"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr))

		assert.Equal(t, uint64(42), actualNum)
		assert.Equal(t, "hello", actualStr)
	})

	t.Run("escaped string values", func(t *testing.T) {
		cases := []struct {
			name  string
			value string
			want  string
		}{
			{"raw literal with escapes", `line 1\nline 2\tend`, "line 1\nline 2\tend"},
			{"interpreted literal with escaped backslashes", "line 1\\nline 2\\tend", "line 1\nline 2\tend"},
			{"raw literal with literal backslashes", `line 1\\nline 2\\tend`, `line 1\nline 2\tend`},
			{"interpreted literal with literal backslashes", "line 1\\\\nline 2\\\\tend", `line 1\nline 2\tend`},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var got string
				row := client.QueryRow(ctx, "SELECT {value:String}", clickhouse.Named("value", tc.value))
				require.NoError(t, row.Scan(&got))
				assert.Equal(t, tc.want, got)
			})
		}

		for _, value := range []string{"line 1\nline 2", "column 1\tcolumn 2"} {
			row := client.QueryRow(ctx, "SELECT {value:String}", clickhouse.Named("value", value))
			require.Error(t, row.Err(), "value %q should be rejected", value)
		}
	})

	t.Run("with identifier type", func(t *testing.T) {
		var actualNum uint64

		row := client.QueryRow(
			ctx,
			"SELECT {column:Identifier} FROM {database:Identifier}.{table:Identifier} LIMIT 1 OFFSET 100;",
			clickhouse.Named("column", "number"),
			clickhouse.Named("database", "system"),
			clickhouse.Named("table", "numbers"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum))

		assert.Equal(t, uint64(100), actualNum)
	})

	t.Run("named args with string and interface supported", func(t *testing.T) {
		var actualNum uint64
		var actualStr string
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			clickhouse.Named("num", 42),
			clickhouse.Named("str", "hello"),
		)
		require.NoError(t, row.Scan(&actualNum, &actualStr))

		assert.Equal(t, uint64(42), actualNum)
		assert.Equal(t, "hello", actualStr)
	})

	t.Run("unsupported arg type", func(t *testing.T) {
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			1234,
			"String",
		)
		require.ErrorIs(t, row.Err(), clickhouse.ErrUnsupportedQueryParameter)
	})

	t.Run("invalid NamedDateValue", func(t *testing.T) {
		row := client.QueryRow(
			ctx,
			"SELECT {ts:DateTime}",
			clickhouse.DateNamed("ts", time.Time{}, clickhouse.Seconds), // zero time
		)
		require.ErrorIs(t, row.Err(), clickhouse.ErrInvalidValueInNamedDateValue)
	})

	t.Run("valid named args", func(t *testing.T) {
		row := client.QueryRow(
			ctx,
			"SELECT {str:String}, {ts:DateTime}",
			clickhouse.Named("str", "hi"),
			clickhouse.DateNamed("ts", time.Now(), clickhouse.Seconds),
		)
		require.NoError(t, row.Err())
	})

	// DateNamed values go out as epoch, so the moment they point to survives
	// whatever timezone the value or the parameter carries. The old
	// wall-clock text was re-read in the parameter's zone, which shifted the
	// stored moment by the zone offset — 9 hours in this case.
	t.Run("DateNamed keeps the instant for non-UTC times", func(t *testing.T) {
		tokyo := time.FixedZone("Asia/Tokyo", 9*3600)
		in := time.Date(2020, 1, 2, 12, 0, 0, 0, tokyo) // == 03:00:00 UTC

		var got time.Time
		row := client.QueryRow(ctx,
			"SELECT {d:DateTime('UTC')}",
			clickhouse.DateNamed("d", in, clickhouse.Seconds),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&got))
		assert.True(t, got.Equal(in), "want instant %s, got %s", in.UTC(), got.UTC())
	})

	// The scale decides the precision: milliseconds round-trip into a
	// matching DateTime64, and the Seconds scale drops them.
	t.Run("DateNamed scale controls sub-second precision", func(t *testing.T) {
		in := time.Date(2020, 1, 2, 3, 4, 5, 123000000, time.UTC)

		var got time.Time
		row := client.QueryRow(ctx,
			"SELECT {d:DateTime64(3, 'UTC')}",
			clickhouse.DateNamed("d", in, clickhouse.MilliSeconds),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&got))
		assert.True(t, got.Equal(in), "want instant %s, got %s", in.UTC(), got.UTC())

		row = client.QueryRow(ctx,
			"SELECT {d:DateTime('UTC')}",
			clickhouse.DateNamed("d", in, clickhouse.Seconds),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&got))
		assert.True(t, got.Equal(in.Truncate(time.Second)), "want truncated instant %s, got %s", in.Truncate(time.Second).UTC(), got.UTC())
	})

	t.Run("Stringer values", func(t *testing.T) {
		id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		addr := netip.MustParseAddr("10.0.0.1")

		var (
			gotUUID  uuid.UUID
			gotIP    net.IP
			gotArray []uuid.UUID
		)
		row := client.QueryRow(
			ctx,
			"SELECT {id:UUID}, {addr:IPv4}, {ids:Array(UUID)}",
			clickhouse.Named("id", id),
			clickhouse.Named("addr", addr),
			clickhouse.Named("ids", []uuid.UUID{id}),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&gotUUID, &gotIP, &gotArray))

		assert.Equal(t, id, gotUUID)
		assert.Equal(t, "10.0.0.1", gotIP.String())
		assert.Equal(t, []uuid.UUID{id}, gotArray)
	})

	t.Run("with bind backwards compatibility", func(t *testing.T) {
		var actualNum uint8
		var actualStr string
		row := client.QueryRow(
			ctx,
			"SELECT @num, @str",
			clickhouse.Named("num", 42),
			clickhouse.Named("str", "hello"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr))

		assert.Equal(t, uint8(42), actualNum)
		assert.Equal(t, "hello", actualStr)
	})
}
