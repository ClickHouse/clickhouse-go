
package issues

import (
	"context"
	"testing"
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestIssue260(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	)
	require.NoError(t, err)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE issue_260 (
			Col1 Nullable(DateTime('UTC'))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_260")
	}()
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_260")
	require.NoError(t, err)
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Send())
	var col1 *time.Time
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM issue_260").Scan(&col1))
	assert.Nil(t, col1)
}
