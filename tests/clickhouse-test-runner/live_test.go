package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// liveEnv enables the tests that need a ClickHouse server on localhost
// (native port 9000, HTTP port 8123), such as `make up` starts.
const liveEnv = "CLICKHOUSE_TEST_RUNNER_LIVE"

func requireLive(t *testing.T) {
	t.Helper()
	if os.Getenv(liveEnv) != "1" {
		t.Skipf("set %s=1 to run the tests against a ClickHouse server on localhost", liveEnv)
	}
}

// serverTSV returns the server's own TabSeparated text for a query.
func serverTSV(t *testing.T, query string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://localhost:8123/?"+url.Values{"default_format": {"TabSeparated"}}.Encode(), strings.NewReader(query))
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	return string(body)
}

func runShim(t *testing.T, protocol, query string) (stdout, stderr string, code int) {
	t.Helper()
	t.Setenv(protocolEnv, protocol)
	var out, errOut bytes.Buffer
	code = run([]string{"--query", query}, strings.NewReader(""), &out, &errOut)
	return out.String(), errOut.String(), code
}

// TestLiveRenderMatchesServer checks the TabSeparated renderer: for each
// expression, the text that the shim writes from the values that
// clickhouse-go decoded must be the same as the server's text.
func TestLiveRenderMatchesServer(t *testing.T) {
	requireLive(t)
	exprs := []string{
		"toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
		"toInt128(-170141183460469231731687303715884105728), toUInt256(115792089237316195423570985008687907853269984665640564039457584007913129639935)",
		"arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0.])",
		"toFloat32(0.1), toFloat32(3.4028235e38), toBFloat16(1.5)",
		"toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal256('1.5', 10)",
		"'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01', toFixedString('ab', 4)",
		"true, false, NULL, CAST(NULL AS Nullable(String))",
		"toDate('2020-01-02'), toDate32('1900-01-01'), toDateTime('2020-01-01 10:00:00', 'UTC')",
		"toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo'), toDateTime64('2020-01-01 00:00:00.120', 3), toDateTime64('2020-01-01 00:00:00', 9, 'Asia/Tokyo')",
		"toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), toIPv6('2001:db8::1')",
		"[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)]",
		"(1, 'x', NULL), CAST((1, 'x') AS Tuple(a Int32, b String)), [(1, 'a')]",
		"map('k', 1, 'a', 2), map(1, [2]), map('x', map('y', 1))",
		"CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = 1, 'b' = 2))]",
		"toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')]",
		"INTERVAL 3 DAY, toIntervalSecond(-5)",
		"toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3)",
		"(1.5, 2.5)::Point, [(1, 2), (3, 4)]::Ring",
		"number, toString(number) FROM numbers(3)",
		// A column or alias named `format` is not the FORMAT clause.
		"1 AS format ORDER BY format DESC",
		"1 AS format ORDER BY format ASC",
		"format FROM (SELECT 1 AS format)",
		"1 AS format FROM numbers(1)",
		"1 AS format, format x",
	}
	for _, protocol := range []string{"native", "http"} {
		for _, expr := range exprs {
			t.Run(protocol+"/"+expr, func(t *testing.T) {
				query := "SELECT " + expr
				want := serverTSV(t, query)
				got, stderr, code := runShim(t, protocol, query)
				require.Equal(t, 0, code, stderr)
				assert.Equal(t, want, got)
			})
		}
	}
}

// TestLiveSessionState checks that `SET` and temporary tables last for the
// whole invocation, also after an error that a test hint expects.
// TestLiveFormatIdentifiers checks queries with a table, a CTE, a column or
// an alias named `format`: the shim must not read them as a FORMAT clause.
func TestLiveFormatIdentifiers(t *testing.T) {
	requireLive(t)
	queries := []string{
		"WITH format AS (SELECT 1 AS x) SELECT x FROM format src",
		"WITH format AS (SELECT 1 AS x) SELECT * FROM format src FORMAT TSV",
		"SELECT 1 AS format, format x",
		"SELECT 1 AS format ORDER BY format DESC",
	}
	for _, protocol := range []string{"native", "http"} {
		for _, query := range queries {
			t.Run(protocol+"/"+query, func(t *testing.T) {
				want := serverTSV(t, strings.TrimSuffix(query, " FORMAT TSV"))
				got, stderr, code := runShim(t, protocol, query)
				require.Equal(t, 0, code, stderr)
				assert.Equal(t, want, got)
			})
		}
	}
}

// TestLiveImplicitSelectMutation checks that a DELETE under
// `implicit_select = 1` does not run as a query: over native, a query
// without a result resets the session, and the temporary table is lost.
func TestLiveImplicitSelectMutation(t *testing.T) {
	requireLive(t)
	for _, protocol := range []string{"native", "http"} {
		t.Run(protocol, func(t *testing.T) {
			// A lightweight DELETE needs a table of the database; the
			// temporary table shows whether the session survived.
			table := "default.live_implicit_select_" + protocol
			script := strings.Join([]string{
				"SET implicit_select = 1;",
				"CREATE TEMPORARY TABLE tmp (x UInt8);",
				"INSERT INTO tmp VALUES (5);",
				"DROP TABLE IF EXISTS " + table + ";",
				"CREATE TABLE " + table + " (x UInt8) ENGINE = MergeTree ORDER BY x;",
				"INSERT INTO " + table + " VALUES (1), (2);",
				"DELETE FROM " + table + " WHERE x = 1;",
				// replace() is the string function: a query.
				"replace('abc', 'b', 'x');",
				"SELECT sum(x) FROM " + table + ";",
				"SELECT sum(x) FROM tmp;",
				"DROP TABLE " + table + ";",
			}, "\n")
			got, stderr, code := runShim(t, protocol, script)
			require.Equal(t, 0, code, stderr)
			assert.Equal(t, "axc\n2\n5\n", got)
		})
	}
}

func TestLiveSessionState(t *testing.T) {
	requireLive(t)
	script := strings.Join([]string{
		"SET max_threads = 3;",
		"SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }",
		"SELECT getSetting('max_threads');",
		"CREATE TEMPORARY TABLE t (x UInt8);",
		"INSERT INTO t VALUES (1), (2);",
		"INSERT INTO t FORMAT TSV",
		"3",
		"",
		"SELECT sum(x) FROM t;",
	}, "\n")
	for _, protocol := range []string{"native", "http"} {
		t.Run(protocol, func(t *testing.T) {
			got, stderr, code := runShim(t, protocol, script)
			require.Equal(t, 0, code, stderr)
			assert.Equal(t, "3\n6\n", got)
		})
	}
}

func TestLiveErrors(t *testing.T) {
	requireLive(t)
	for _, protocol := range []string{"native", "http"} {
		t.Run(protocol, func(t *testing.T) {
			// An unexpected error stops the script, writes the error name to
			// stderr, and sets the exit code to the error code.
			got, stderr, code := runShim(t, protocol, "SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;")
			assert.Equal(t, "1\n", got)
			assert.Equal(t, 60, code)
			assert.Contains(t, stderr, "UNKNOWN_TABLE")

			// A query that succeeds although the hint expects an error.
			_, stderr, code = runShim(t, protocol, "SELECT 1; -- { serverError UNKNOWN_TABLE }")
			assert.NotEqual(t, 0, code)
			assert.Contains(t, stderr, "expected serverError UNKNOWN_TABLE")

			// A different error than the hint expects.
			_, stderr, code = runShim(t, protocol, "SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }")
			assert.Equal(t, 60, code, fmt.Sprintf("stderr: %s", stderr))
		})
	}
}
