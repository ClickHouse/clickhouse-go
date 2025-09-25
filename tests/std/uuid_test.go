
package std

import (
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStdUUID(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSNWithSessionID(dsn, "uuid_test_session")
			require.NoError(t, err)

			const ddl = `
			CREATE TEMPORARY TABLE std_test_uuid (
				  Col1 UUID
				, Col2 UUID
			) Engine Memory()
		`

			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_uuid")
			require.NoError(t, err)
			var (
				col1Data = uuid.New()
				col2Data = uuid.New()
			)
			_, err = batch.Exec(col1Data, col2Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 uuid.UUID
				col2 uuid.UUID
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM std_test_uuid").Scan(&col1, &col2))
			assert.Equal(t, col1Data, col1)
			assert.Equal(t, col2Data, col2)

			_, err = conn.Exec("DROP TABLE std_test_uuid")
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		})
	}
}

func TestStdNullableUUID(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSNWithSessionID(dsn, "nullable_uuid_test_session")
			require.NoError(t, err)

			const ddl = `
					CREATE TEMPORARY TABLE std_test_nullable_uuid (
						  Col1 Nullable(UUID)
						, Col2 Nullable(UUID)
					)
				`

			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_nullable_uuid")
			require.NoError(t, err)
			var (
				col1Data = uuid.New()
				col2Data = uuid.New()
			)
			_, err = batch.Exec(col1Data, col2Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 *uuid.UUID
				col2 *uuid.UUID
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM std_test_nullable_uuid").Scan(&col1, &col2))
			assert.Equal(t, col1Data, *col1)
			assert.Equal(t, col2Data, *col2)
			_, err = conn.Exec("TRUNCATE TABLE std_test_nullable_uuid")
			require.NoError(t, err)
			scope, err = conn.Begin()
			require.NoError(t, err)
			batch, err = scope.Prepare("INSERT INTO std_test_nullable_uuid")
			require.NoError(t, err)
			col1Data = uuid.New()
			_, err = batch.Exec(col1Data, nil)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			{
				var (
					col1 *uuid.UUID
					col2 *uuid.UUID
				)
				require.NoError(t, conn.QueryRow("SELECT * FROM std_test_nullable_uuid").Scan(&col1, &col2))
				require.Nil(t, col2)
				assert.Equal(t, col1Data, *col1)
			}

			_, err = conn.Exec("DROP TABLE IF EXISTS std_test_nullable_uuid")
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		})
	}
}
