package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1537_MixedPlaceholdersAndComment(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	// PostgreSQL-style placeholder in query; `?` is in comment and should be ignored
	row := conn.QueryRow(ctx, "SELECT $1 -- some comment with ?", "clickhouse")

	var s string
	err = row.Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "clickhouse", s)
}

