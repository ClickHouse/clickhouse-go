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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssue357(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = ` -- foo.bar DDL comment
		CREATE TEMPORARY TABLE issue_357 (
			  Col1 Int32
			, Col2 DateTime
		)
		`
		defer func() {
			conn.Exec("DROP TABLE issue_357")
		}()
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			const query = ` -- foo.bar Insert comment
				INSERT INTO issue_357
				`
			if batch, err := scope.Prepare(query); assert.NoError(t, err) {
				if _, err := batch.Exec(int32(42), time.Now()); assert.NoError(t, err) {
					if err := scope.Commit(); assert.NoError(t, err) {
						var (
							col1 int32
							col2 time.Time
						)
						if err := conn.QueryRow("SELECT * FROM issue_357").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, int32(42), col1)
						}
					}
				}
			}
		}
	}
}
