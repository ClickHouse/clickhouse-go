
package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
)

func TestIssue412(t *testing.T) {
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
			CREATE TABLE issue_412 (
				Col1 SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_412")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_412")
	require.NoError(t, err)
	datetime := time.Now().Truncate(time.Millisecond)
	require.NoError(t, batch.Append(datetime))
	require.NoError(t, batch.Send())
	var col1 time.Time
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM issue_412").Scan(&col1))
	assert.Equal(t, datetime.UnixNano(), col1.UnixNano())
}
