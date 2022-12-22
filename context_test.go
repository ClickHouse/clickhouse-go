package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {
	// Call context multiple times making sure query options are persisted across calls.
	ctx := Context(context.Background(), WithQueryID("a"))
	ctx = Context(ctx, WithQuotaKey("b"))
	ctx = Context(ctx, WithSettings(Settings{
		"c": "d",
	}))

	opts := queryOptions(ctx)
	require.Equal(t, "a", opts.queryID)
	require.Equal(t, "b", opts.quotaKey)
	require.Equal(t, "d", opts.settings["c"])

	// Call context multiple times with the same query options, making sure the latest is persisted.
	ctx = Context(context.Background(), WithQueryID("a"))
	ctx = Context(ctx, WithQueryID("b"))

	opts = queryOptions(ctx)
	require.Equal(t, "b", opts.queryID)
}
