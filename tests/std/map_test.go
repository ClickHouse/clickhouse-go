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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdMap(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := checkMinServerVersion(conn, 21, 9, 0); err != nil {
					t.Skip(err.Error())
					return
				}
				const ddl = `
		CREATE TABLE test_map (
			  Col1 Map(String, UInt64)
			, Col2 Map(String, UInt64)
			, Col3 Map(String, UInt64)
			, Col4 Array(Map(String, String))
			, Col5 Map(LowCardinality(String), LowCardinality(UInt64))
		) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_map")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_map"); assert.NoError(t, err) {
						var (
							col1Data = map[string]uint64{
								"key_col_1_1": 1,
								"key_col_1_2": 2,
							}
							col2Data = map[string]uint64{
								"key_col_2_1": 10,
								"key_col_2_2": 20,
							}
							col3Data = map[string]uint64{}
							col4Data = []map[string]string{
								map[string]string{"A": "B"},
								map[string]string{"C": "D"},
							}
							col5Data = map[string]uint64{
								"key_col_5_1": 100,
								"key_col_5_2": 200,
							}
						)
						if _, err := batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data); assert.NoError(t, err) {
							if assert.NoError(t, scope.Commit()) {
								var (
									col1 interface{}
									col2 map[string]uint64
									col3 map[string]uint64
									col4 []map[string]string
									col5 map[string]uint64
								)
								if err := conn.QueryRow("SELECT * FROM test_map").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
									assert.Equal(t, col1Data, col1)
									assert.Equal(t, col2Data, col2)
									assert.Equal(t, col3Data, col3)
									assert.Equal(t, col4Data, col4)
									assert.Equal(t, col5Data, col5)
								}
							}
						}
					}
				}
			}
		})
	}
}
