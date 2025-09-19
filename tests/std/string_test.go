
package std

import (
	"database/sql"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestSimpleStdString(t *testing.T) {
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
			const ddl = `CREATE TABLE std_test_string (Col1 String, Col2 Nullable(String)) Engine MergeTree() ORDER BY tuple()`
			conn.Exec("DROP TABLE std_test_string")
			defer func() {
				conn.Exec("DROP TABLE std_test_string")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_string")
			require.NoError(t, err)
			var (
				col1Data = "A"
			)
			for i := 0; i < 10; i++ {
				_, err := batch.Exec(col1Data, nil)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM std_test_string")
			require.NoError(t, err)
			for rows.Next() {
				var (
					col1 any
					col2 sql.NullString
				)
				require.NoError(t, rows.Scan(&col1, &col2))
				assert.Equal(t, col1Data, col1)
				assert.Equal(t, sql.NullString{Valid: false}, col2)
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}
