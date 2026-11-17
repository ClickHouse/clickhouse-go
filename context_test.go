package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
)

func TestContext(t *testing.T) {
	t.Run("query options are persisted across multiple calls",
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

	t.Run("persist maps from parent context",
		func(t *testing.T) {
			// First context
			firstContext := Context(context.Background(), WithSettings(Settings{
				"a": "b",
			}))

			firstOpts := queryOptions(firstContext)
			require.Equal(t, "b", firstOpts.settings["a"])

			// Second context
			secondContext := Context(firstContext, WithSettings(Settings{
				"c": "d",
			}))

			// First context's map was updated by second context
			secondOpts := queryOptions(secondContext)
			require.Equal(t, "d", secondOpts.settings["c"])
		},
	)

	t.Run("settings map initialized when nil",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithSettings(nil))

			opts := queryOptions(ctx)
			require.NotNil(t, opts.settings)
		},
	)

	t.Run("settings map not nil for empty context",
		func(t *testing.T) {
			ctx := context.Background()

			opts := queryOptions(ctx)
			require.NotNil(t, opts.settings)
		},
	)

	t.Run("copy maps when reading queryOptions",
		func(t *testing.T) {
			// First context
			firstContext := Context(context.Background(), WithSettings(Settings{
				"key": "a",
			}))
			// Get first unique copy of options
			firstOpts := queryOptions(firstContext)

			// Second context
			secondContext := Context(firstContext, WithSettings(Settings{
				"key": "b",
			}))
			// Get second unique copy of options
			secondOpts := queryOptions(secondContext)

			// First options was not changed by map override from second context
			require.Equal(t, "a", firstOpts.settings["key"])

			// Update values in first options
			firstOpts.settings["key"] = "c"

			// Second options map should not be changed
			require.Equal(t, "b", secondOpts.settings["key"])
		},
	)

	t.Run("queryOptionsAsync valid for ClickHouse context",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithStdAsync(true))

			asyncOpt := queryOptionsAsync(ctx)
			require.True(t, asyncOpt.ok)
			require.True(t, asyncOpt.wait)
		},
	)
	t.Run("queryOptionsAsync invalid for empty context",
		func(t *testing.T) {
			ctx := context.Background()

			asyncOpt := queryOptionsAsync(ctx)
			require.False(t, asyncOpt.ok)
			require.False(t, asyncOpt.wait)
		},
	)

	t.Run("queryOptionsUserLocation valid for ClickHouse context",
		func(t *testing.T) {
			ctx := Context(context.Background(), WithUserLocation(time.UTC))

			loc := queryOptionsUserLocation(ctx)
			require.Equal(t, time.UTC, loc)
		},
	)
	t.Run("queryOptionsUserLocation nil for empty context",
		func(t *testing.T) {
			ctx := context.Background()

			loc := queryOptionsUserLocation(ctx)
			require.Nil(t, loc)
		},
	)

	t.Run("correctly appends client info on multiple calls",
		func(t *testing.T) {
			// First context
			firstContext := Context(context.Background(), WithClientInfo(ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "product",
						Version: "1.0.0",
					},
				},
				Comment: []string{"comment_a"},
			}))

			firstOpts := queryOptions(firstContext)
			require.Len(t, firstOpts.clientInfo.Products, 1)
			require.Equal(t, "product", firstOpts.clientInfo.Products[0].Name)
			require.Equal(t, "1.0.0", firstOpts.clientInfo.Products[0].Version)
			require.Len(t, firstOpts.clientInfo.Comment, 1)
			require.Equal(t, "comment_a", firstOpts.clientInfo.Comment[0])

			// Second context
			secondContext := Context(firstContext, WithClientInfo(ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "product2",
						Version: "2.0.0",
					},
				},
				Comment: []string{"comment_b"},
			}))

			// Product and comment values should be merged from the first+second contexts
			secondOpts := queryOptions(secondContext)

			// Check first context values still present
			require.Len(t, secondOpts.clientInfo.Products, 2)
			require.Equal(t, "product", secondOpts.clientInfo.Products[0].Name)
			require.Equal(t, "1.0.0", secondOpts.clientInfo.Products[0].Version)
			require.Len(t, secondOpts.clientInfo.Comment, 2)
			require.Equal(t, "comment_a", secondOpts.clientInfo.Comment[0])

			// Check second context values present
			require.Len(t, secondOpts.clientInfo.Products, 2)
			require.Equal(t, "product2", secondOpts.clientInfo.Products[1].Name)
			require.Equal(t, "2.0.0", secondOpts.clientInfo.Products[1].Version)
			require.Len(t, secondOpts.clientInfo.Comment, 2)
			require.Equal(t, "comment_b", secondOpts.clientInfo.Comment[1])
		},
	)
}

func TestContextJWTPreserved(t *testing.T) {
	// Regression: queryOptions() returns a clone of the context's QueryOptions,
	// and clone() must carry the per-query JWT set via WithJWT. Otherwise the
	// HTTPS transport reads an empty token and silently falls back to the
	// connection's basic-auth credentials.
	t.Run("clone preserves WithJWT token", func(t *testing.T) {
		ctx := Context(context.Background(), WithJWT("tok-123"))
		require.Equal(t, "tok-123", queryOptions(ctx).jwt)
	})

	t.Run("jwt survives alongside other options", func(t *testing.T) {
		ctx := Context(context.Background(), WithJWT("tok-123"), WithQueryID("q1"))
		ctx = Context(ctx, WithQuotaKey("k1"))
		opts := queryOptions(ctx)
		require.Equal(t, "tok-123", opts.jwt)
		require.Equal(t, "q1", opts.queryID)
		require.Equal(t, "k1", opts.quotaKey)
	})
}

func TestWithoutProfileEvents(t *testing.T) {
	t.Run("sets send_profile_events=0", func(t *testing.T) {
		ctx := Context(context.Background(), WithoutProfileEvents())
		opts := queryOptions(ctx)
		require.Equal(t, 0, opts.settings["send_profile_events"])
	})

	t.Run("initializes nil settings map", func(t *testing.T) {
		var opts QueryOptions
		WithoutProfileEvents()(&opts)
		require.NotNil(t, opts.settings)
		require.Equal(t, 0, opts.settings["send_profile_events"])
	})
}

// TestContextExternalTables is a regression test for
// https://github.com/ClickHouse/clickhouse-go/issues/1724.
func TestContextExternalTables(t *testing.T) {
	newTable := func(t *testing.T, name string) *ext.Table {
		t.Helper()
		table, err := ext.NewTable(name, ext.Column("id", "UInt64"))
		require.NoError(t, err)
		return table
	}
	// requireTables compares pointers, because two tables can have the same name.
	requireTables := func(t *testing.T, ctx context.Context, want ...*ext.Table) {
		t.Helper()
		got := queryOptions(ctx).external
		require.Len(t, got, len(want))
		for i := range want {
			require.Samef(t, want[i], got[i], "external table %d (%q) is not the expected table", i, got[i].Name())
		}
	}

	t.Run("a child context does not change the tables of a sibling context", func(t *testing.T) {
		// Use three separate calls. If WithExternalTable appends in place, the slice
		// of the parent then has spare capacity, and the two children write to the
		// same element.
		t1, t2, t3 := newTable(t, "t1"), newTable(t, "t2"), newTable(t, "t3")
		parent := Context(context.Background(), WithExternalTable(t1))
		parent = Context(parent, WithExternalTable(t2))
		parent = Context(parent, WithExternalTable(t3))

		ids1, ids2 := newTable(t, "ids"), newTable(t, "ids")
		child1 := Context(parent, WithExternalTable(ids1))
		child2 := Context(parent, WithExternalTable(ids2))

		requireTables(t, child1, t1, t2, t3, ids1)
		requireTables(t, child2, t1, t2, t3, ids2)
		requireTables(t, parent, t1, t2, t3)
	})

	t.Run("a table replaces the table with the same name from the parent context", func(t *testing.T) {
		other, ids1, ids2 := newTable(t, "other"), newTable(t, "ids"), newTable(t, "ids")
		parent := Context(context.Background(), WithExternalTable(other, ids1))
		child := Context(parent, WithExternalTable(ids2))

		requireTables(t, child, other, ids2)
		requireTables(t, parent, other, ids1)
	})

	t.Run("a table replaces the table with the same name from an earlier option", func(t *testing.T) {
		ids1, ids2 := newTable(t, "ids"), newTable(t, "ids")
		ctx := Context(context.Background(), WithExternalTable(ids1), WithExternalTable(ids2))

		requireTables(t, ctx, ids2)
	})

	t.Run("one call keeps two tables with the same name", func(t *testing.T) {
		ids1, ids2 := newTable(t, "ids"), newTable(t, "ids")
		ctx := Context(context.Background(), WithExternalTable(ids1, ids2))

		requireTables(t, ctx, ids1, ids2)
	})
}
