
package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue517(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	var result []struct {
		Col1 uint64 `ch:"number"`
	}
	require.NoError(t, conn.Select(ctx, &result, "SELECT number FROM system.numbers LIMIT 10"))
	require.Len(t, result, 10)
	require.NoError(t, conn.Select(ctx, &result, "SELECT number FROM system.numbers LIMIT 5"))
	require.Len(t, result, 5)
	for i, v := range result {
		assert.Equal(t, uint64(i), v.Col1)
	}
}
