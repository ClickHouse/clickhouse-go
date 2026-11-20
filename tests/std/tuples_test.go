package std

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var testDate, _ = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2022-05-25 17:20:57 +0100 WEST")

func TestTuple(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, tlsConfig, nil)
	require.NoError(t, err)
	loc, err := time.LoadLocation("Europe/Lisbon")
	require.NoError(t, err)
	localTime := testDate.In(loc)

	if !CheckMinServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE std_test_tuple (
			  Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime('Europe/Lisbon'))
			, Col3 Tuple(name1 DateTime('Europe/Lisbon'), name2 FixedString(2), name3 Map(String, String))
			, Col4 Array(Array( Tuple(String, Int64) ))
			, Col5 Tuple(LowCardinality(String),           Array(LowCardinality(String)))
			, Col6 Tuple(LowCardinality(Nullable(String)), Array(LowCardinality(Nullable(String))))
			, Col7 Tuple(String, Int64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec("DROP TABLE std_test_tuple")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO std_test_tuple")
	require.NoError(t, err)
	var (
		col1Data = []any{"A", int64(42)}
		col2Data = []any{"B", int8(1), localTime.Truncate(time.Second)}
		col3Data = map[string]any{
			"name1": localTime.Truncate(time.Second),
			"name2": "CH",
			"name3": map[string]string{
				"key": "value",
			},
		}
		col4Data = [][][]any{
			{
				{"Hi", int64(42)},
			},
		}
		col5Data = []any{
			"LCString",
			[]string{"A", "B", "C"},
		}
		str      = "LCString"
		col6Data = []any{
			&str,
			[]*string{&str, nil, &str},
		}
		col7Data = &[]any{"C", int64(42)}
	)
	_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 any
		col2 any
		// col3 is a named tuple - we can use map
		col3 any
		col4 any
		col5 any
		col6 any
		col7 any
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM std_test_tuple").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.NoError(t, err)
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, *col7Data, col7)
}

func TestStdTupleNested(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Interface", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, false, url.Values{})
			require.NoError(t, err)
			t.Cleanup(func() {
				if err := conn.Close(); err != nil {
					t.Logf("failed to close the connection: %v", err)
				}
			})
			if !CheckMinServerVersion(conn, 21, 9, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}

			var (
				inTuple      any
				inArray      any
				arrayInTuple any
				inNamed      any
			)
			require.NoError(t, conn.QueryRow(`SELECT
				  ((1, 'a'), 2)
				, [((1, 'a'), 2)]
				, ([(1, 2)], 3)
				, CAST(((1, 'a'), 2) AS Tuple(a Tuple(UInt8, String), b UInt8))
			`).Scan(&inTuple, &inArray, &arrayInTuple, &inNamed))
			assert.Equal(t, []any{[]any{uint8(1), "a"}, uint8(2)}, inTuple)
			assert.Equal(t, [][]any{{[]any{uint8(1), "a"}, uint8(2)}}, inArray)
			assert.Equal(t, []any{[][]any{{uint8(1), uint8(2)}}, uint8(3)}, arrayInTuple)
			assert.Equal(t, map[string]any{"a": []any{uint8(1), "a"}, "b": uint8(2)}, inNamed)

			if !CheckMinServerVersion(conn, 24, 3, 0) {
				return
			}
			var (
				empty any
				after uint8
			)
			require.NoError(t, conn.QueryRow("SELECT (tuple(), [tuple()]), toUInt8(1)").Scan(&empty, &after))
			assert.Equal(t, []any{[]any{}, [][]any{{}}}, empty)
			assert.Equal(t, uint8(1), after)
		})
	}
}
