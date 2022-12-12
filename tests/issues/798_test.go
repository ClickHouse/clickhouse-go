package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
)

func Test798(t *testing.T) {
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
			CREATE TABLE test_issue_798 (
				  Col1 Bool
				, Col2 Bool
				, Col3 Array(Bool)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_798")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_798")
	require.NoError(t, err)
	require.NoError(t, batch.Append(true, false, []bool{true, false, true}))
	require.NoError(t, batch.Send())
	// test resend
	require.ErrorIs(t, batch.Send(), clickhouse.ErrBatchAlreadySent)
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_issue_798")
	require.NoError(t, err)
	// test empty batch
	require.NoError(t, batch.Send())
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_issue_798")
	// append invalid batch
	require.Error(t, batch.Append("true", false, []bool{true, false, true}))
	// send invalid batch
	require.ErrorIs(t, batch.Flush(), clickhouse.ErrBatchInvalid)
	require.ErrorIs(t, batch.Send(), clickhouse.ErrBatchInvalid)
	// test append, send, append
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_issue_798")
	require.NoError(t, batch.Append(true, false, []bool{true, false, true}))
	require.NoError(t, batch.Send())
	require.ErrorIs(t, batch.Append(true, false, []bool{true, false, true}), clickhouse.ErrBatchAlreadySent)
}

func writeRows(prepareSQL string, rows [][]interface{}, conn clickhouse.Conn) (err error) {
	batch, err := conn.PrepareBatch(context.Background(), prepareSQL)
	if err != nil {
		return err
	}
	defer func() {
		if batch != nil {
			_ = batch.Abort()
		}
	}()
	for _, row := range rows {
		batch.Append(row...)
	}
	return batch.Send()
}

func Test798Concurrent(t *testing.T) {

	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	const ddl = `
			CREATE TABLE test_issue_798 (
				  Col1 Bool
				, Col2 Bool
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_798")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))

	require.NoError(t, err)
	todo, done := int64(1000), int64(-1)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for {
				index := atomic.AddInt64(&done, 1)
				if index >= todo {
					wg.Done()
					break
				}
				writeRows("INSERT INTO test_issue_798", [][]interface{}{{true, false}, {false, true}}, conn)
			}
		}()
	}
	wg.Wait()

}
