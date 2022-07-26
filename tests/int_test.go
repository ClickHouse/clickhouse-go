package tests

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestSimpleInt(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})
	)
	require.NoError(t, err)
	if err := CheckMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = "CREATE TABLE test_int (`1` Int64) Engine Memory"
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_int")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_int")
	require.NoError(t, err)
	require.Error(t, batch.Append(222))
}

func TestNullableInt(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})
	)
	require.NoError(t, err)
	if err := checkMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = "CREATE TABLE test_int (col1 Int64, col2 Nullable(Int64), col3 Int32, col4 Nullable(Int32), col5 Int16, col6 Nullable(Int16)) Engine Memory"
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_int")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_int")
	require.NoError(t, err)
	col1Data := sql.NullInt64{Int64: 1, Valid: true}
	col2Data := sql.NullInt64{Int64: 0, Valid: false}
	col3Data := sql.NullInt32{Int32: 2, Valid: true}
	col4Data := sql.NullInt32{Int32: 0, Valid: false}
	col5Data := sql.NullInt16{Int16: 3, Valid: true}
	col6Data := sql.NullInt16{Int16: 0, Valid: false}
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data))
	require.NoError(t, batch.Send())
	var (
		col1 sql.NullInt64
		col2 sql.NullInt64
		col3 sql.NullInt32
		col4 sql.NullInt32
		col5 sql.NullInt16
		col6 sql.NullInt16
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_int").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	require.Equal(t, col1Data, col1)
	require.Equal(t, col2Data, col2)
	require.Equal(t, col3Data, col3)
	require.Equal(t, col4Data, col4)
	require.Equal(t, col5Data, col5)
	require.Equal(t, col6Data, col6)
}
