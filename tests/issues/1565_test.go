package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1565(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	row := conn.QueryRow(ctx, "SELECT map(['success', 'failure'], [10, 5]) as value")
	require.ErrorContains(t, row.Err(), "clickhouse: unsupported column type")
}
