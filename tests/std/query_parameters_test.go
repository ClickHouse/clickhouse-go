package std

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
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
