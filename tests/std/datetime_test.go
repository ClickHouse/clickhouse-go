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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdDateTime(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				const ddl = `
			CREATE TABLE test_datetime (
				  Col1 DateTime
				, Col2 DateTime('Europe/Moscow')
				, Col3 DateTime('Europe/London')
				, Col4 Nullable(DateTime('Europe/Moscow'))
				, Col5 Array(DateTime('Europe/Moscow'))
				, Col6 Array(Nullable(DateTime('Europe/Moscow')))
			) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_datetime")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_datetime"); assert.NoError(t, err) {
						datetime := time.Now().Truncate(time.Second)
						if _, err := batch.Exec(
							datetime,
							datetime,
							datetime,
							&datetime,
							[]time.Time{datetime, datetime},
							[]*time.Time{&datetime, nil, &datetime},
						); assert.NoError(t, err) {
							if err := scope.Commit(); assert.NoError(t, err) {
								var (
									col1 time.Time
									col2 time.Time
									col3 time.Time
									col4 *time.Time
									col5 []time.Time
									col6 []*time.Time
								)
								if err := conn.QueryRow("SELECT * FROM test_datetime").Scan(&col1, &col2, &col3, &col4, &col5, &col6); assert.NoError(t, err) {
									assert.Equal(t, datetime, col1)
									assert.Equal(t, datetime.Unix(), col2.Unix())
									assert.Equal(t, datetime.Unix(), col3.Unix())
									if name == "Http" {
										// Native Format over HTTP works with revision 0
										// Clickhouse before 54337 revision don't support Time Zone
										// So, it removes Time Zone if revision less than 54337
										if assert.Equal(t, "Local", col2.Location().String()) {
											assert.Equal(t, "Local", col3.Location().String())
										}
									} else {
										if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
											assert.Equal(t, "Europe/London", col3.Location().String())
										}
									}
									assert.Equal(t, datetime.Unix(), col4.Unix())
									if assert.Len(t, col5, 2) {
										assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
										assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
									}
									if assert.Len(t, col6, 3) {
										assert.Nil(t, col6[1])
										assert.NotNil(t, col6[0])
										assert.NotNil(t, col6[2])
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
