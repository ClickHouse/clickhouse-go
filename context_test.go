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
