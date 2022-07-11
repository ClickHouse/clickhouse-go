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

func TestStdBool(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := checkMinServerVersion(conn, 21, 12, 0); err != nil {
					t.Skip(err.Error())
					return
				}
				const ddl = `
			CREATE TABLE test_bool (
				    Col1 Bool
				  , Col2 Bool
				  , Col3 Array(Bool)
				  , Col4 Nullable(Bool)
				  , Col5 Array(Nullable(Bool))
			) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_bool")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_bool"); assert.NoError(t, err) {
						var val bool
						if _, err := batch.Exec(true, false, []bool{true, false, true}, nil, []*bool{&val, nil, &val}); assert.NoError(t, err) {
							if err := scope.Commit(); assert.NoError(t, err) {
								var (
									col1 bool
									col2 bool
									col3 []bool
									col4 *bool
									col5 []*bool
								)
								if err := conn.QueryRow("SELECT * FROM test_bool").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
									assert.Equal(t, true, col1)
									assert.Equal(t, false, col2)
									assert.Equal(t, []bool{true, false, true}, col3)
									if assert.Nil(t, col4) {
										assert.Equal(t, []*bool{&val, nil, &val}, col5)
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
