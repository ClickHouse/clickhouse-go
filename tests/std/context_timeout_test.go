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

package std

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestStdContextStdTimeout(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			connect, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			t.Run("query which triggers timeout", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(3)"); assert.NotNil(t, row) {
					var a, b int
					if err := row.Scan(&a, &b); assert.Error(t, err) {
						clickhouse_tests.AssertIsTimeoutError(t, err)
					}
				}
			})
			t.Run("query which returns in time", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
					var value, value2 int
					if assert.NoError(t, row.Scan(&value, &value2)) {
						assert.Equal(t, 1, value)
					}
				}
			})
		})
	}
}
