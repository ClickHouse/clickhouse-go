package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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
			assert.Equal(t, "a", opts.queryID)
			assert.Equal(t, "b", opts.quotaKey)
			assert.Equal(t, "d", opts.settings["c"])
		},
	)

	t.Run("call context multiple times making sure query options are persisted across calls",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithQueryID("a"))
			ctx = Context(ctx, WithQueryID("b"))

			opts := queryOptions(ctx)
			assert.Equal(t, "b", opts.queryID)
		},
	)
}
