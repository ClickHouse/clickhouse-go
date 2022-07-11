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
	"database/sql"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdTemporaryTable(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			ctx := context.Background()
			if name == "Http" {
				ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
					"session_id": "test_session",
				}))
			}

			if connect, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				const ddl = `
						CREATE TEMPORARY TABLE test_temporary_table (
							ID UInt64
						);
					`
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					connect.ExecContext(ctx, "DROP TABLE IF EXISTS test_temporary_table")
					if _, err := tx.ExecContext(ctx, ddl); assert.NoError(t, err) {
						if _, err := tx.ExecContext(ctx, "INSERT INTO test_temporary_table (ID) SELECT number AS ID FROM system.numbers LIMIT 10"); assert.NoError(t, err) {
							if rows, err := tx.QueryContext(ctx, "SELECT ID AS ID FROM test_temporary_table"); assert.NoError(t, err) {
								var count int
								for rows.Next() {
									var num int
									if err := rows.Scan(&num); !assert.NoError(t, err) {
										return
									}
									count++
								}
								if _, err = tx.QueryContext(ctx, "SELECT ID AS ID1 FROM test_temporary_table"); assert.NoError(t, err) {
									if _, err = connect.Query("SELECT ID AS ID2 FROM test_temporary_table"); assert.Error(t, err) {
										if name == "Http" {
											assert.Contains(t, err.Error(), "Code: 60")
										} else {
											if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
												assert.Equal(t, int32(60), exception.Code)
											}
										}
									}
								}
								if assert.Equal(t, int(10), count) {
									if assert.NoError(t, tx.Commit()) {
										assert.NoError(t, connect.Close())
									}
								}
							}
						}
					}
				}
			}
		})
	}
}
