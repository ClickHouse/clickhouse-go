package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {
	t.Run("call context multiple times making sure query options are persisted across calls",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithQueryID("a"))
			ctx = Context(ctx, WithQuotaKey("b"))
			ctx = Context(ctx, WithSettings(Settings{
				"c": "d",
			}))

			opts := queryOptions(ctx)
			require.Equal(t, "a", opts.queryID)
			require.Equal(t, "b", opts.quotaKey)
			require.Equal(t, "d", opts.settings["c"])
		},
	)

	t.Run("call context multiple times making sure query options are persisted across calls",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithQueryID("a"))
			ctx = Context(ctx, WithQueryID("b"))

			opts := queryOptions(ctx)
			require.Equal(t, "b", opts.queryID)
		},
	)
}
