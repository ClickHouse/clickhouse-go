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

package issues

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test470PR(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE issue_470_pr (
			Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec("DROP TABLE issue_470_pr")
		}()
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO issue_470_pr"); assert.NoError(t, err) {
				if _, err := batch.Exec(nil); assert.Error(t, err) {
					assert.Contains(t, err.Error(), "converting <nil> to Array(String) is unsupported")
				}
			}
		}
	}
}
