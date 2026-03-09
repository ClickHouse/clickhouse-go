package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
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

func TestInjectSendProfileEvents(t *testing.T) {
	newServer := proto.Version{Major: 25, Minor: 11, Patch: 0}
	oldServer := proto.Version{Major: 25, Minor: 10, Patch: 0}

	t.Run("no listener and new server injects setting", func(t *testing.T) {
		opts := QueryOptions{settings: make(Settings)}
		opts.injectSendProfileEvents(nil, newServer)
		require.Equal(t, 0, opts.settings["send_profile_events"])
	})

	t.Run("listener registered does not inject setting", func(t *testing.T) {
		opts := QueryOptions{settings: make(Settings)}
		opts.events.profileEvents = func([]ProfileEvent) {}
		opts.injectSendProfileEvents(nil, newServer)
		_, ok := opts.settings["send_profile_events"]
		require.False(t, ok)
	})

	t.Run("connection-level setting not overridden", func(t *testing.T) {
		connSettings := Settings{"send_profile_events": true}
		opts := QueryOptions{settings: make(Settings)}
		opts.injectSendProfileEvents(connSettings, newServer)
		_, ok := opts.settings["send_profile_events"]
		require.False(t, ok)
	})

	t.Run("query-level setting not overridden", func(t *testing.T) {
		opts := QueryOptions{settings: Settings{"send_profile_events": 1}}
		opts.injectSendProfileEvents(nil, newServer)
		require.Equal(t, 1, opts.settings["send_profile_events"])
	})

	t.Run("old server does not inject setting", func(t *testing.T) {
		opts := QueryOptions{settings: make(Settings)}
		opts.injectSendProfileEvents(nil, oldServer)
		_, ok := opts.settings["send_profile_events"]
		require.False(t, ok)
	})

	t.Run("nil settings map is initialized", func(t *testing.T) {
		opts := QueryOptions{}
		opts.injectSendProfileEvents(nil, newServer)
		require.NotNil(t, opts.settings)
		require.Equal(t, 0, opts.settings["send_profile_events"])
	})
}
