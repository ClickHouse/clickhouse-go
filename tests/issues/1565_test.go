package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue1565(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	row := conn.QueryRow(ctx, "SELECT map(['success', 'failure'], [10, 5]) as value")
	require.ErrorContains(t, row.Err(), "clickhouse: unsupported column type")
}
