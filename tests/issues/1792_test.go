package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// controlCharCase is one value to bind and expect back from the server. The
// raw string form covers the string/*string binding branches; asBytes covers
// the []byte/*[]byte branches, which must escape identically.
type controlCharCase struct {
	name    string
	value   string
	asBytes bool
}

// controlCharCases is the shared set of test cases used by all 1792 tests.
//
// A top-level String sent via Named is TSV-escaped by the driver, so every
// value below — including raw control characters that were previously
// truncated or rejected on the native TCP path — must round-trip byte-for-byte
// on both protocols (#1792).
var controlCharCases = []controlCharCase{
	{name: "plain string", value: "hello world"},
	{name: "tab character", value: "hello\tworld"},
	{name: "newline character", value: "hello\nworld"},
	{name: "carriage return", value: "hello\rworld"},
	{name: "backslash", value: `hello\world`},
	{name: "single quote", value: "it's"},
	{name: "backslash followed by t (not a tab)", value: `hello\tworld`},
	{name: "nul byte", value: "hello\x00world"},
	{name: "mixed control characters", value: "tab:\there\nnewline\\backslash'quote"},
	{name: "tab character in byte slice", value: "hello\tworld", asBytes: true},
	{name: "newline character in byte slice", value: "hello\nworld", asBytes: true},
	{name: "backslash in byte slice", value: `hello\world`, asBytes: true},
}

// stringParamArg is the argument to bind for tc: the raw string, or the same
// content as a []byte to exercise the byte-slice binding branch.
func stringParamArg(tc controlCharCase) any {
	if tc.asBytes {
		return []byte(tc.value)
	}
	return tc.value
}

// Test1792 verifies that String query parameters containing control characters
// (tab, newline, carriage return, backslash, NUL) round-trip on both the
// native TCP and HTTP protocols.
//
// The ClickHouse server decodes parameter values as TSV-escaped text
// (deserializeTextEscaped); the native protocol additionally wraps them in a
// quoted Field dump (readQuoted). The driver escapes control characters once
// where the raw string enters, so callers can pass ordinary Go strings.
func Test1792(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			t.Skipf("server-side query parameters require ClickHouse 22.8+")
		}

		for _, tc := range controlCharCases {
			t.Run(tc.name, func(t *testing.T) {
				var got string
				row := conn.QueryRow(ctx, "SELECT {str:String}",
					clickhouse.Named("str", stringParamArg(tc)))
				require.NoError(t, row.Scan(&got))
				assert.Equal(t, tc.value, got)
			})
		}
	})
}

// Test1792HTTP verifies the same round-trip over the HTTP protocol with an
// explicit session id, covering the URL query-string path.
func Test1792HTTP(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionHTTP("issues", t.Name(), clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
		t.Skipf("server-side query parameters require ClickHouse 22.8+")
	}

	ctx := context.Background()
	for _, tc := range controlCharCases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			row := conn.QueryRow(ctx, "SELECT {str:String}",
				clickhouse.Named("str", stringParamArg(tc)))
			require.NoError(t, row.Scan(&got))
			assert.Equal(t, tc.value, got)
		})
	}
}

// stdRoundTrip runs the control-character round-trip through the
// database/sql interface using clickhouse.Named().
func stdRoundTrip(t *testing.T, protocol clickhouse.Protocol) {
	db, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	if !clickhouse_std_tests.CheckMinServerVersion(db, 22, 8, 0) {
		t.Skipf("server-side query parameters require ClickHouse 22.8+")
	}

	ctx := context.Background()
	for _, tc := range controlCharCases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			row := db.QueryRowContext(ctx, "SELECT {str:String}",
				clickhouse.Named("str", stringParamArg(tc)))
			require.NoError(t, row.Scan(&got))
			assert.Equal(t, tc.value, got)
		})
	}
}

// Test1792Std verifies the round-trip through database/sql over HTTP.
func Test1792Std(t *testing.T) {
	stdRoundTrip(t, clickhouse.HTTP)
}

// Test1792StdTCP verifies the round-trip through database/sql over native TCP.
func Test1792StdTCP(t *testing.T) {
	stdRoundTrip(t, clickhouse.Native)
}
