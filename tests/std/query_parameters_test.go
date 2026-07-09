package std

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestQueryParameters(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	connectionString := fmt.Sprintf("http://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60", env.Host, env.HttpPort, env.Username, env.Password)
	if useSSL {
		connectionString = fmt.Sprintf("https://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)
	}
	dsns := map[string]string{"Http": connectionString}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSN(dsn)
			require.NoError(t, err)

			t.Run("with named arguments", func(t *testing.T) {
				var actualNum uint64
				var actualStr string
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					clickhouse.Named("num", "42"),
					clickhouse.Named("str", "hello"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum, &actualStr))

				assert.Equal(t, uint64(42), actualNum)
				assert.Equal(t, "hello", actualStr)
			})

			t.Run("named args with string and interface supported", func(t *testing.T) {
				var actualNum uint64
				var actualStr string
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					clickhouse.Named("num", 42),
					clickhouse.Named("str", "hello"),
				)
				require.NoError(t, row.Scan(&actualNum, &actualStr))

				assert.Equal(t, uint64(42), actualNum)
				assert.Equal(t, "hello", actualStr)
			})

			t.Run("with identifier type", func(t *testing.T) {
				var actualNum uint64

				row := conn.QueryRow(
					"SELECT {column:Identifier} FROM {database:Identifier}.{table:Identifier} LIMIT 1 OFFSET 100;",
					clickhouse.Named("column", "number"),
					clickhouse.Named("database", "system"),
					clickhouse.Named("table", "numbers"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum))

				assert.Equal(t, uint64(100), actualNum)
			})

			t.Run("unsupported arg type", func(t *testing.T) {
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					1234,
					"String",
				)
				require.ErrorIs(t, row.Err(), clickhouse.ErrUnsupportedQueryParameter)
			})

			t.Run("invalid NamedDateValue", func(t *testing.T) {
				row := conn.QueryRow(
					"SELECT {ts:DateTime}",
					clickhouse.DateNamed("ts", time.Time{}, clickhouse.Seconds), // zero time
				)
				require.ErrorIs(t, row.Err(), clickhouse.ErrInvalidValueInNamedDateValue)
			})

			t.Run("valid named args", func(t *testing.T) {
				row := conn.QueryRow(
					"SELECT {str:String}, {ts:DateTime}",
					clickhouse.Named("str", "hi"),
					clickhouse.DateNamed("ts", time.Now(), clickhouse.Seconds),
				)
				require.NoError(t, row.Err())
			})

			// DateNamed values go out as epoch, so the moment they point to
			// survives whatever timezone the value or the parameter carries.
			// The old wall-clock text was re-read in the parameter's zone,
			// which shifted the stored moment by the zone offset — 9 hours
			// here. Same assertions as the native suite: parameters travel
			// differently per protocol (URL-encoded on HTTP, Field dump on
			// native), so each transport proves its own path.
			t.Run("DateNamed keeps the instant for non-UTC times", func(t *testing.T) {
				tokyo := time.FixedZone("Asia/Tokyo", 9*3600)
				in := time.Date(2020, 1, 2, 12, 0, 0, 0, tokyo) // == 03:00:00 UTC

				var got time.Time
				row := conn.QueryRow(
					"SELECT {d:DateTime('UTC')}",
					clickhouse.DateNamed("d", in, clickhouse.Seconds),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&got))
				assert.True(t, got.Equal(in), "want instant %s, got %s", in.UTC(), got.UTC())
			})

			// The scale decides the precision: milliseconds round-trip into
			// a matching DateTime64, and the Seconds scale drops them.
			t.Run("DateNamed scale controls sub-second precision", func(t *testing.T) {
				in := time.Date(2020, 1, 2, 3, 4, 5, 123000000, time.UTC)

				var got time.Time
				row := conn.QueryRow(
					"SELECT {d:DateTime64(3, 'UTC')}",
					clickhouse.DateNamed("d", in, clickhouse.MilliSeconds),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&got))
				assert.True(t, got.Equal(in), "want instant %s, got %s", in.UTC(), got.UTC())

				row = conn.QueryRow(
					"SELECT {d:DateTime('UTC')}",
					clickhouse.DateNamed("d", in, clickhouse.Seconds),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&got))
				assert.True(t, got.Equal(in.Truncate(time.Second)), "want truncated instant %s, got %s", in.Truncate(time.Second).UTC(), got.UTC())
			})

			t.Run("with bind backwards compatibility", func(t *testing.T) {
				var actualNum uint8
				var actualStr string
				row := conn.QueryRow(
					"SELECT @num, @str",
					clickhouse.Named("num", 42),
					clickhouse.Named("str", "hello"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum, &actualStr))

				assert.Equal(t, uint8(42), actualNum)
				assert.Equal(t, "hello", actualStr)
			})
		})
	}
}
