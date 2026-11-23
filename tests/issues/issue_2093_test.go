package issues

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

type issue2093Value struct {
	value    any
	expected string
}

type issue2093Rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

func TestIssue2093LowCardinalityByteSlicesRoundTrip(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		t.Run("native API", func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 19, 11, 0) {
				t.Skip("LowCardinality requires ClickHouse 19.11+")
			}

			ctx := context.Background()
			const table = "issue_2093_native"
			createIssue2093Table(t, func(query string) error { return conn.Exec(ctx, query) }, table)
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
			require.NoError(t, err)
			appendIssue2093Values(t, func(id uint8, value, nullable any, array []any) error {
				return batch.Append(id, value, nullable, array)
			})
			require.NoError(t, batch.Send())

			rows, err := conn.Query(ctx, "SELECT id, value, nullable_value, array_value FROM "+table+" ORDER BY id")
			require.NoError(t, err)
			assertIssue2093Rows(t, rows)
		})

		t.Run("database/sql API", func(t *testing.T) {
			env, err := clickhouse_tests.GetTestEnvironment("issues")
			require.NoError(t, err)
			if !proto.CheckMinVersion(proto.Version{Major: 19, Minor: 11}, env.Version) {
				t.Skip("LowCardinality requires ClickHouse 19.11+")
			}

			options := clickhouse_tests.ClientOptionsFromEnv(env, clickhouse_tests.TestClientDefaultSettings(env), protocol == clickhouse.HTTP)
			if protocol == clickhouse.HTTP {
				options.Settings["wait_end_of_query"] = 1
			}
			var db *sql.DB = clickhouse.OpenDB(&options)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			ctx := context.Background()
			require.NoError(t, db.PingContext(ctx))
			const table = "issue_2093_database_sql"
			createIssue2093Table(t, func(query string) error {
				_, err := db.ExecContext(ctx, query)
				return err
			}, table)

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			txFinished := false
			t.Cleanup(func() {
				if !txFinished {
					if err := tx.Rollback(); err != nil {
						t.Errorf("rollback transaction: %v", err)
					}
				}
			})
			stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+table)
			require.NoError(t, err)
			stmtClosed := false
			t.Cleanup(func() {
				if !stmtClosed {
					if err := stmt.Close(); err != nil {
						t.Errorf("close prepared statement: %v", err)
					}
				}
			})
			appendIssue2093Values(t, func(id uint8, value, nullable any, array []any) error {
				_, err := stmt.ExecContext(ctx, id, value, nullable, array)
				return err
			})
			require.NoError(t, stmt.Close())
			stmtClosed = true
			require.NoError(t, tx.Commit())
			txFinished = true

			rows, err := db.QueryContext(ctx, "SELECT id, value, nullable_value, array_value FROM "+table+" ORDER BY id")
			require.NoError(t, err)
			assertIssue2093Rows(t, rows)
		})
	})
}

func createIssue2093Table(t *testing.T, exec func(string) error, table string) {
	t.Helper()
	require.NoError(t, exec("DROP TABLE IF EXISTS "+table))
	require.NoError(t, exec("CREATE TABLE "+table+` (
		id UInt8,
		value LowCardinality(String),
		nullable_value LowCardinality(Nullable(String)),
		array_value Array(LowCardinality(String))
	) ENGINE = MergeTree ORDER BY id`))
	t.Cleanup(func() { require.NoError(t, exec("DROP TABLE IF EXISTS "+table)) })
}

func appendIssue2093Values(t *testing.T, appendRow func(uint8, any, any, []any) error) {
	t.Helper()
	byteSlice := []byte("value")
	rawMessage := json.RawMessage("value")
	byteSlicePointer := []byte("value")
	rawMessagePointer := json.RawMessage("value")
	inputs := []issue2093Value{
		{value: []byte("value"), expected: "value"},
		{value: "value", expected: "value"},
		{value: json.RawMessage("value"), expected: "value"},
		{value: &byteSlicePointer, expected: "value"},
		{value: &rawMessagePointer, expected: "value"},
		{value: []byte("other"), expected: "other"},
	}
	array := []any{
		[]byte("array-value"),
		json.RawMessage("array-value"),
		"array-value",
		&byteSlice,
		&rawMessage,
		[]byte("array-other"),
	}
	for i, input := range inputs {
		require.NoError(t, appendRow(uint8(i), input.value, input.value, array))
	}
	require.NoError(t, appendRow(uint8(len(inputs)), "null-row", nil, array))
}

func assertIssue2093Rows(t *testing.T, rows issue2093Rows) {
	t.Helper()
	defer func() { require.NoError(t, rows.Close()) }()
	inputs := []issue2093Value{
		{expected: "value"},
		{expected: "value"},
		{expected: "value"},
		{expected: "value"},
		{expected: "value"},
		{expected: "other"},
		{expected: "null-row"},
	}
	expectedArray := []string{"array-value", "array-value", "array-value", "array-value", "array-value", "array-other"}
	rowCount := 0
	for rows.Next() {
		i := rowCount
		require.Less(t, i, len(inputs), "unexpected extra row")
		var (
			id       uint8
			value    string
			nullable *string
			array    []string
		)
		require.NoError(t, rows.Scan(&id, &value, &nullable, &array))
		require.Equal(t, uint8(i), id)
		require.Equal(t, inputs[i].expected, value)
		require.Equal(t, expectedArray, array)
		if i == len(inputs)-1 {
			require.Nil(t, nullable)
		} else {
			require.NotNil(t, nullable)
			require.Equal(t, inputs[i].expected, *nullable)
		}
		rowCount++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(inputs), rowCount)
}
