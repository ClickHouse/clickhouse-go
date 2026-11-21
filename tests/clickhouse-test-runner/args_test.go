package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseArgsSQLTestCommandLine(t *testing.T) {
	// The command line that tests/clickhouse-test uses for a .sql test.
	opts, err := parseArgs([]string{
		"client", "--host=127.0.0.1", "--port=9000", "--no-secure", "--database=test_abc",
		"--send_logs_level=warning", "--multiquery", "--log_comment", "00001_select_1.sql-test_abc",
		"--max_block_size=1", "--allow_repeated_settings", "--client_logs_file=/tmp/x-fatal",
		"--param_CLICKHOUSE_DATABASE=test_abc",
	})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", opts.Host)
	assert.Equal(t, 9000, opts.Port)
	assert.Equal(t, "test_abc", opts.Database)
	assert.False(t, opts.Secure)
	assert.Nil(t, opts.Query)
	assert.Equal(t, map[string]string{
		"send_logs_level": "warning",
		"log_comment":     "00001_select_1.sql-test_abc",
	}, opts.Settings)
	assert.Equal(t, []string{"send_logs_level", "log_comment"}, opts.SettingOrder)
	assert.Equal(t, map[string]string{"CLICKHOUSE_DATABASE": "test_abc"}, opts.Params)
}

func TestParseArgsShortOptions(t *testing.T) {
	opts, err := parseArgs([]string{"-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s"})
	require.NoError(t, err)
	assert.Equal(t, "db", opts.Host)
	assert.Equal(t, "me", opts.User)
	assert.Equal(t, "x", opts.Database)
	require.NotNil(t, opts.Query)
	assert.Equal(t, "SELECT 1", *opts.Query)
	assert.True(t, opts.Secure)
}

func TestParseArgsSettingValues(t *testing.T) {
	opts, err := parseArgs([]string{"--max_threads", "-1", "--max-insert-threads", "2", "--query=SELECT 1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"max_threads": "-1", "max_insert_threads": "2"}, opts.Settings)
}

func TestParseArgsErrors(t *testing.T) {
	_, err := parseArgs([]string{"--port=abc"})
	assert.Error(t, err)
	_, err = parseArgs([]string{"--query"})
	assert.Error(t, err)
}

func TestRedactPassword(t *testing.T) {
	for _, tc := range []struct{ in, want []string }{
		{
			in:   []string{"--user", "u", "--password", "secret", "--password=secret2", "--query", "SELECT 1", "--password"},
			want: []string{"--user", "u", "--password", "***", "--password=***", "--query", "SELECT 1", "--password"},
		},
		// The value of --password can start with "-".
		{in: []string{"--password", "-q", "--max_threads", "1"}, want: []string{"--password", "***", "--max_threads", "1"}},
		// The first --password is the value of -u; the second one takes the password.
		{in: []string{"-u", "--password", "--password", "secret"}, want: []string{"-u", "--password", "--password", "***"}},
		// Contrast: the value of --query, and the arguments after "--", are no password.
		{in: []string{"--query", "--password", "x"}, want: []string{"--query", "--password", "x"}},
		{in: []string{"--", "--password", "x"}, want: []string{"--", "--password", "x"}},
	} {
		got := redactPassword(tc.in)
		assert.Equal(t, tc.want, got, tc.in)
		opts, err := parseArgs(tc.in)
		if err == nil && opts.Password != "" {
			assert.NotContains(t, got, opts.Password, tc.in)
		}
	}
}

func TestHTTPPort(t *testing.T) {
	t.Setenv("CLICKHOUSE_PORT_TCP", "")
	t.Setenv("CLICKHOUSE_PORT_HTTP", "")
	assert.Equal(t, 8123, httpPort(0, false))
	assert.Equal(t, 8123, httpPort(9000, false))
	assert.Equal(t, 18123, httpPort(18123, false))
	assert.Equal(t, 8443, httpPort(9440, true))

	t.Setenv("CLICKHOUSE_PORT_TCP", "19000")
	t.Setenv("CLICKHOUSE_PORT_HTTP", "18124")
	assert.Equal(t, 18124, httpPort(19000, false))
	assert.Equal(t, 9000, httpPort(9000, false))
}

func TestExtractFromConfig(t *testing.T) {
	var out bytes.Buffer
	assert.Equal(t, 0, run([]string{"extract-from-config", "--try", "--config", "x.xml", "--key", "listen_host"}, nil, &out, nil))
	assert.Equal(t, "127.0.0.1\n", out.String())

	out.Reset()
	assert.Equal(t, 0, run([]string{"extract-from-config", "--key=tcp_port"}, nil, &out, nil))
	assert.Empty(t, out.String())
}

func TestParseUseDatabase(t *testing.T) {
	params := map[string]string{"CLICKHOUSE_DATABASE": "test_x"}
	for in, want := range map[string]string{
		"USE db1;":                              "db1",
		"use `my db`":                           "my db",
		"-- c\nUSE DATABASE db2":                "db2",
		"USE {CLICKHOUSE_DATABASE:Identifier};": "test_x",
		"USE \"db3\"":                           "db3",
		"USE /* c */ db4 -- d\n;":               "db4",
		"USE DATABASE /* c */ `db 5`;":          "db 5",
	} {
		got, ok := parseUseDatabase(in, params)
		assert.True(t, ok, in)
		assert.Equal(t, want, got, in)
	}
	_, ok := parseUseDatabase("USE db1, db2", params)
	assert.False(t, ok)
}

func TestParseSetAssignmentsWithComments(t *testing.T) {
	assert.Equal(t, map[string]string{"max_threads": "3", "dialect": "kusto"},
		parseSetAssignments("SET /* c */ max_threads /* d */ = 3 -- note\n, dialect = 'kusto' # x\n;"))
	// A comment marker inside a quoted value is not a comment.
	assert.Equal(t, map[string]string{"log_comment": "a -- b"},
		parseSetAssignments("SET log_comment = 'a -- b';"))
}

func TestParseSetAssignments(t *testing.T) {
	assert.Equal(t, map[string]string{"dialect": "kusto", "max_threads": "1", "x": "(1, 2)"},
		parseSetAssignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);"))
}
