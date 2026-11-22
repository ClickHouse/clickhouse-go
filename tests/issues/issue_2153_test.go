package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// values2153 are the values that each test appends to all columns of one row.
// A driver.Valuer whose Value method returns nil is NULL.
var values2153 = []any{
	nil,
	(*string)(nil),
	sql.NullString{},
	&sql.NullString{},
	sql.Null[string]{},
	&sql.Null[string]{},
	sql.NullString{String: "a", Valid: true},
	sql.Null[string]{V: "b", Valid: true},
	"c",
}

// expected2153 are the values that the server stores for values2153 in a
// `Nullable(String)` column. nil is NULL.
var expected2153 = []any{nil, nil, nil, nil, nil, nil, "a", "b", "c"}

type row2153 struct {
	ID  uint8          `ch:"id"`
	N   sql.NullString `ch:"n"`
	LCN sql.NullString `ch:"lcn"`
	S   sql.NullString `ch:"s"`
	LC  sql.NullString `ch:"lc"`
}

// Test2153 verifies that a driver.Valuer whose Value method returns nil, such as
// sql.NullString{}, is NULL in a `LowCardinality(Nullable(String))` column, as it is
// in a `Nullable(String)` column. In a `LowCardinality(String)` column it is an empty
// string, as it is in a `String` column.
// See https://github.com/ClickHouse/clickhouse-go/issues/2153.
func Test2153(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		ctx := context.Background()

		tableName := fmt.Sprintf("test_2153_%v", protocol)
		createTable2153(t, conn, tableName)

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
		require.NoError(t, err)
		for i, v := range values2153 {
			require.NoError(t, batch.Append(uint8(i), v, v, v, v), "append %#v", v)
		}
		require.NoError(t, batch.AppendStruct(&row2153{ID: uint8(len(values2153))}))
		require.NoError(t, batch.Send())

		requireRows2153(t, conn, tableName, append(append([]any{}, expected2153...), nil))
	})
}

// Test2153Std is Test2153 for the database/sql API.
func Test2153Std(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		tableName := fmt.Sprintf("test_2153_std_%v", protocol)
		createTable2153(t, conn, tableName)

		tx, err := db.Begin()
		require.NoError(t, err)
		stmt, err := tx.Prepare("INSERT INTO " + tableName)
		require.NoError(t, err)
		for i, v := range values2153 {
			_, err := stmt.Exec(uint8(i), v, v, v, v)
			require.NoError(t, err, "exec %#v", v)
		}
		require.NoError(t, tx.Commit())

		requireRows2153(t, conn, tableName, expected2153)
	})
}

func createTable2153(t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id  UInt8,
		n   Nullable(String),
		lcn LowCardinality(Nullable(String)),
		s   String,
		lc  LowCardinality(String)
	) ENGINE = MergeTree ORDER BY tuple()`, tableName)))
	t.Cleanup(func() {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
			t.Logf("DROP TABLE %s failed: %v", tableName, err)
		}
	})
}

// requireRows2153 reads the table and compares each row with expected. The
// LowCardinality columns must have the same values as the plain columns.
func requireRows2153(t *testing.T, conn driver.Conn, tableName string, expected []any) {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT id, n, lcn, s, lc FROM "+tableName+" ORDER BY id")
	require.NoError(t, err)
	t.Cleanup(func() { rows.Close() })

	value := func(s *string) any {
		if s == nil {
			return nil
		}
		return *s
	}
	var count int
	for rows.Next() {
		var (
			id     uint8
			n, lcn *string
			s, lc  string
		)
		require.NoError(t, rows.Scan(&id, &n, &lcn, &s, &lc))
		require.Less(t, int(id), len(expected))
		empty := ""
		if expected[id] != nil {
			empty = expected[id].(string)
		}
		assert.Equal(t, expected[id], value(n), "row %d: Nullable(String)", id)
		assert.Equal(t, expected[id], value(lcn), "row %d: LowCardinality(Nullable(String))", id)
		assert.Equal(t, empty, s, "row %d: String", id)
		assert.Equal(t, empty, lc, "row %d: LowCardinality(String)", id)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, len(expected), count)
}
