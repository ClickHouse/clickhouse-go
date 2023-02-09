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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func TestAsyncBatch(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			if !CheckMinServerVersion(conn, 21, 12, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}

			_, err = conn.Exec(`DROP TABLE IF EXISTS example`)
			require.NoError(t, err)
			_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			  Col1 UInt8
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col5 Map(String, UInt8)
			, Col6 Array(String)
			, Col7 Tuple(String, UInt8, Array(Map(String, String)))
			, Col8 DateTime
		) Engine = Memory
	`)
			require.NoError(t, err)

			queryCtx := clickhouse.Context(context.Background(), clickhouse.WithAsync())

			scope, err := conn.Begin()

			assert.NoError(t, err)
			batch, err := scope.PrepareContext(queryCtx, "INSERT INTO example")
			assert.NoError(t, err)
			for i := 0; i < 1000; i++ {
				_, err := batch.ExecContext(
					queryCtx,
					uint8(42),
					"ClickHouse", "Inc",
					uuid.New(),
					map[string]uint8{"key": 1},             // Map(String, UInt8)
					[]string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
					[]interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
						"String Value", uint8(5), []map[string]string{
							map[string]string{"key": "value"},
							map[string]string{"key": "value"},
							map[string]string{"key": "value"},
						},
					},
					time.Now(),
				)

				assert.NoError(t, err)
			}

			assert.NoError(t, scope.Commit())
		})
	}
}
