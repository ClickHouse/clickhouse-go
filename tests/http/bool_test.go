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

package http

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHttpBool(t *testing.T) {
	if conn, err := sql.Open("clickhousehttp", "http://127.0.0.1:8123?dial_timeout=1s&compress=true"); assert.NoError(t, err) {

		const ddl = `
			CREATE TABLE test_bool (
				    Col1 Bool 
				    ,Col2 Bool 
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
				if _, err := batch.Exec(true, false); assert.NoError(t, err) {
					if err := scope.Commit(); assert.NoError(t, err) {
						rows, err := conn.QueryContext(
							context.Background(), "SELECT * FROM test_bool",
						)
						assert.NoError(t, err)
						for rows.Next() {
							var (
								col1 bool
								col2 bool
							)
							if err := rows.Scan(&col1); err != nil {
								assert.NoError(t, err)
							}
							assert.Equal(t, true, col1)
							assert.Equal(t, false, col2)
						}
						assert.NoError(t, rows.Close())
					}
				}
			}
		}
	}
}
