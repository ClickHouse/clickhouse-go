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
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdArray(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
		) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_array")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_array"); assert.NoError(t, err) {
						var (
							timestamp = time.Now().Truncate(time.Second)
							col1Data  = []string{"A", "b", "c"}
							col2Data  = [][]uint32{
								[]uint32{1, 2},
								[]uint32{3, 87},
								[]uint32{33, 3, 847},
							}
							col3Data = [][][]time.Time{
								[][]time.Time{
									[]time.Time{
										timestamp,
										timestamp,
										timestamp,
										timestamp,
									},
								},
								[][]time.Time{
									[]time.Time{
										timestamp,
										timestamp,
										timestamp,
									},
									[]time.Time{
										timestamp,
										timestamp,
									},
								},
							}
						)
						for i := 0; i < 10; i++ {
							if _, err := batch.Exec(col1Data, col2Data, col3Data); !assert.NoError(t, err) {
								return
							}
						}
						require.NoError(t, scope.Commit())
						if rows, err := conn.Query("SELECT * FROM test_array"); assert.NoError(t, err) {
							for rows.Next() {
								var (
									col1 interface{}
									col2 [][]uint32
									col3 [][][]time.Time
								)
								if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
									assert.Equal(t, col1Data, col1)
									assert.Equal(t, col2Data, col2)
									assert.Equal(t, col3Data, col3)
								}
							}
							require.NoError(t, rows.Close())
							require.NoError(t, rows.Err())
						}

					}
				}
			}
		})
	}
}
