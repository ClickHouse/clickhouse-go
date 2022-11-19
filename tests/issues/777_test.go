package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestInsertNullableString(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_nullable_string_insert (
			  Col1 String
			, Col2 Nullable(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nullable_string_insert")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	const baseValues = `
		INSERT INTO test_nullable_string_insert (Col1, Col2) VALUES ('Val1', 'Val2'), ('Val11', NULL)
		`
	require.NoError(t, conn.Exec(ctx, baseValues))

	rows, err := conn.Query(ctx, "SELECT * FROM test_nullable_string_insert")
	require.NoError(t, err)
	defer func(rows driver.Rows) {
		_ = rows.Close()
	}(rows)

	records := make([][]any, 0)
	for rows.Next() {
		record := make([]any, 0, len(rows.ColumnTypes()))
		for _, ct := range rows.ColumnTypes() {
			record = append(record, reflect.New(ct.ScanType()).Interface())
		}
		err = rows.Scan(record...)
		require.NoError(t, err)

		records = append(records, record)
	}
	require.Greater(t, len(records), 0)

	// Try to insert records in the same format as queried above
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nullable_string_insert")
	require.NoError(t, err)

	for _, r := range records {
		err = batch.Append(r...)
		require.NoError(t, err)
	}

	err = batch.Send()
	require.NoError(t, err)
}
