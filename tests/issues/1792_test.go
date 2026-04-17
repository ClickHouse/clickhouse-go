package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test1792 verifies that String query parameters containing control characters
// (tab, newline, carriage return, backslash, single quote) are correctly encoded
// when sent via the native TCP protocol.
//
// The ClickHouse server decodes parameter values through two stages:
//  1. readQuoted: decodes escape sequences inside single-quoted strings
//  2. deserializeTextEscaped: interprets TSV-escaped sequences
//
// The client must double-encode control characters so the round-trip preserves them.
func Test1792(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionTCP("issues", clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, nil)
	require.NoError(t, err)

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}

	ctx := context.Background()

	cases := []struct {
		name  string
		value string
	}{
		{name: "tab character", value: "hello\tworld"},
		{name: "newline character", value: "hello\nworld"},
		{name: "carriage return", value: "hello\rworld"},
		{name: "backslash", value: `hello\world`},
		{name: "single quote", value: "it's"},
		{name: "backslash followed by t (not a tab)", value: `hello\tworld`},
		{name: "mixed control characters", value: "tab:\there\nnewline\\backslash'quote"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chCtx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
				"str": tc.value,
			}))
			var got string
			row := conn.QueryRow(chCtx, "SELECT {str:String}")
			require.NoError(t, row.Err())
			require.NoError(t, row.Scan(&got))
			assert.Equal(t, tc.value, got)
		})
	}
}
