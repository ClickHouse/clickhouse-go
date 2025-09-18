
package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue578(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	)
	require.NoError(t, err)
	assert.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO non_existent_table")
	assert.Error(t, err)

	if batch != nil {
		batch.Abort()
	}
}
