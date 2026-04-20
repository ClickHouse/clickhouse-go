package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controlCharCases is the shared set of test cases used by all 1792 tests.
var controlCharCases = []struct {
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
		t.Skipf("unsupported clickhouse version")
	}

	ctx := context.Background()

	for _, tc := range controlCharCases {
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

// Test1792HTTP verifies that String query parameters containing control characters
// round-trip correctly when sent via the HTTP protocol.
// Over HTTP, parameters are URL-encoded as param_<name>=<value> query string entries.
func Test1792HTTP(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionHTTP("issues", t.Name(), clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, nil)
	require.NoError(t, err)

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
		t.Skipf("unsupported clickhouse version")
	}

	ctx := context.Background()

	for _, tc := range controlCharCases {
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

// Test1792Std verifies that String query parameters containing control characters
// round-trip correctly through the database/sql interface using clickhouse.Named().
func Test1792Std(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, env.HttpPort)},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	defer db.Close()

	if !checkStdMinVersion(db, 22, 8, 0) {
		t.Skipf("unsupported clickhouse version")
	}

	for _, tc := range controlCharCases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			row := db.QueryRow(
				"SELECT {str:String}",
				clickhouse.Named("str", tc.value),
			)
			require.NoError(t, row.Err())
			require.NoError(t, row.Scan(&got))
			assert.Equal(t, tc.value, got)
		})
	}
}

// checkStdMinVersion returns true if the connected server meets the minimum version requirement.
func checkStdMinVersion(db *sql.DB, major, minor, patch uint64) bool {
	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		return false
	}
	var v proto.Version
	fmt.Sscanf(version, "%d.%d.%d", &v.Major, &v.Minor, &v.Patch)
	return proto.CheckMinVersion(proto.Version{Major: major, Minor: minor, Patch: patch}, v)
}
