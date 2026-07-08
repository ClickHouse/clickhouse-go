package std

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1898MapQueryParameter checks the fix for #1898 through the
// database/sql interface: a Go map sent as a `Map(K, V)` server-side query
// parameter must be formatted in the `{'k':v}` text format, because the
// server rejects the `map('k', v)` SQL-function syntax with
// CANNOT_PARSE_INPUT_ASSERTION_FAILED. The std driver ends up in the same
// formatting code as the native client, but this makes sure the second API
// surface is covered too.
func TestIssue1898MapQueryParameter(t *testing.T) {
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
		"SELECT {m:Map(String, Bool)} = map('a', true)",
		clickhouse.Named("m", map[string]bool{"a": true}),
	)
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&ok))
	require.True(t, ok)
}
