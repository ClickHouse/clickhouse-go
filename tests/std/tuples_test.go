package std

import (
	"crypto/tls"
	"fmt"
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

// nested named tuples with Array and Map members round-trip through OpenDB,
// binding the values both as maps and as structs
func TestNamedTupleNested(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, tlsConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE std_test_tuple_nested (
				Col1 Tuple(user_id UInt32, profile Tuple(age UInt8, email String))
				, Col2 Tuple(data Array(Int32), metadata Map(String, String))
			) Engine MergeTree() ORDER BY tuple()
			`
	defer func() {
		conn.Exec("DROP TABLE std_test_tuple_nested")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)

	type profile struct {
		Age   uint8  `ch:"age"`
		Email string `ch:"email"`
	}
	type userWithProfile struct {
		UserID  uint32  `ch:"user_id"`
		Profile profile `ch:"profile"`
	}
	type dataWithMetadata struct {
		Data     []int32           `ch:"data"`
		Metadata map[string]string `ch:"metadata"`
	}
	var (
		col1MapData = map[string]any{
			"user_id": uint32(123),
			"profile": map[string]any{
				"age":   uint8(30),
				"email": "john@example.com",
			},
		}
		col2MapData = map[string]any{
			"data":     []int32{1, 2, 3, 4, 5},
			"metadata": map[string]string{"key1": "value1", "key2": "value2"},
		}
		col1StructData = userWithProfile{
			UserID:  uint32(456),
			Profile: profile{Age: 25, Email: "jane@example.com"},
		}
		col2StructData = dataWithMetadata{
			Data:     []int32{6, 7, 8, 9, 10},
			Metadata: map[string]string{"key3": "value3", "key4": "value4"},
		}
	)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO std_test_tuple_nested")
	require.NoError(t, err)
	_, err = batch.Exec(col1MapData, col2MapData)
	require.NoError(t, err)
	_, err = batch.Exec(col1StructData, col2StructData)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())

	var (
		col1 any
		col2 any
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM std_test_tuple_nested WHERE Col1.user_id = 123").Scan(&col1, &col2))
	assert.JSONEq(t, ToJson(col1MapData), ToJson(col1))
	assert.JSONEq(t, ToJson(col2MapData), ToJson(col2))

	col1StructExpected := map[string]any{
		"user_id": uint32(456),
		"profile": map[string]any{"age": uint8(25), "email": "jane@example.com"},
	}
	col2StructExpected := map[string]any{
		"data":     []int32{6, 7, 8, 9, 10},
		"metadata": map[string]string{"key3": "value3", "key4": "value4"},
	}
	var (
		col1Struct any
		col2Struct any
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM std_test_tuple_nested WHERE Col1.user_id = 456").Scan(&col1Struct, &col2Struct))
	assert.JSONEq(t, ToJson(col1StructExpected), ToJson(col1Struct))
	assert.JSONEq(t, ToJson(col2StructExpected), ToJson(col2Struct))
}
