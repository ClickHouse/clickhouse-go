package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
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
	require.ErrorIs(t, batch.Send(), clickhouse.ErrBatchInvalid)
	// test append, send, append
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_issue_798")
	require.NoError(t, batch.Append(true, false, []bool{true, false, true}))
	require.NoError(t, batch.Send())
	require.ErrorIs(t, batch.Append(true, false, []bool{true, false, true}), clickhouse.ErrBatchAlreadySent)
}
