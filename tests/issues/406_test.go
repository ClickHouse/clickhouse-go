
package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
)

func TestIssue406(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE issue_406 (
				Col1 Tuple(Array(Int32), Array(Int32))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_406")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_406")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		[]any{
			[]int32{1, 2, 3, 4, 5},
			[]int32{5, 1, 2, 3, 4},
		},
	))
	require.NoError(t, batch.Send())
	var col1 []any
	require.NoError(t,
		conn.QueryRow(ctx, "SELECT * FROM issue_406").Scan(&col1))
	assert.Equal(t, []any{
		[]int32{1, 2, 3, 4, 5},
		[]int32{5, 1, 2, 3, 4},
	}, col1)
}
