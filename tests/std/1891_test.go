package std

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1891ArrayBoolQueryParameter pins the #1891 fix for the database/sql
// surface: a []bool passed as an Array(Bool) server-side query parameter must
// render as `true`/`false` (not `1`/`0`, which the server rejects with
// CANNOT_READ_ARRAY_FROM_TEXT). The std driver reaches the same
// bindQueryOrAppendParameters -> formatValue path as the native client via
// rebind, so this guards that second API surface explicitly.
func TestIssue1891ArrayBoolQueryParameter(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	dsn := fmt.Sprintf("http://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60", env.Host, env.HttpPort, env.Username, env.Password)
	if useSSL {
		dsn = fmt.Sprintf("https://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)
	}
	conn, err := GetConnectionFromDSN(dsn)
	require.NoError(t, err)
	defer conn.Close()

	var ok bool
	row := conn.QueryRow(
		"SELECT hasAll([true, false], {vals:Array(Bool)})",
		clickhouse.Named("vals", []bool{true, false}),
	)
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&ok))
	require.True(t, ok)
}
