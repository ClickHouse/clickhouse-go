// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
}
