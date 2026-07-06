package std

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1891ArrayBoolQueryParameter checks the fix for #1891 through the
// database/sql interface: a []bool sent as an `Array(Bool)` server-side query
// parameter must be formatted as `true`/`false`, because the server rejects
// `1`/`0` with CANNOT_READ_ARRAY_FROM_TEXT. The std driver ends up in the
// same formatting code as the native client, but this makes sure the second
// API surface is covered too.
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

	if !CheckMinServerVersion(conn, 22, 8, 0) {
		t.Skip("server-side query parameters require ClickHouse 22.8+")
	}

	var ok bool
	row := conn.QueryRow(
		"SELECT hasAll([true, false], {vals:Array(Bool)})",
		clickhouse.Named("vals", []bool{true, false}),
	)
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&ok))
	require.True(t, ok)
}
