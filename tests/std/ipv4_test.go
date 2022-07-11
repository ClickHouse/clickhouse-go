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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdIPv4(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				const ddl = `
			CREATE TABLE test_ipv4 (
				  Col1 IPv4
				, Col2 IPv4
				, Col3 Nullable(IPv4)
				, Col4 Array(IPv4)
				, Col5 Array(Nullable(IPv4))
			) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_ipv4")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_ipv4"); assert.NoError(t, err) {
						var (
							col1Data = net.ParseIP("127.0.0.1")
							col2Data = net.ParseIP("8.8.8.8")
							col3Data = col1Data
							col4Data = []net.IP{col1Data, col2Data}
							col5Data = []*net.IP{&col1Data, nil, &col2Data}
						)
						if _, err := batch.Exec(col1Data, col2Data, &col3Data, &col4Data, &col5Data); assert.NoError(t, err) {
							if assert.NoError(t, scope.Commit()) {
								var (
									col1 net.IP
									col2 net.IP
									col3 *net.IP
									col4 []net.IP
									col5 []*net.IP
								)
								if err := conn.QueryRow("SELECT * FROM test_ipv4").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
									assert.Equal(t, col1Data.To4(), col1)
									assert.Equal(t, col2Data.To4(), col2)
									assert.Equal(t, col3Data.To4(), *col3)
									if assert.Len(t, col4, 2) {
										assert.Equal(t, col1Data.To4(), col4[0])
										assert.Equal(t, col2Data.To4(), col4[1])
									}
									if assert.Len(t, col5, 3) {
										if assert.Nil(t, col5[1]) {
											assert.Equal(t, col1Data.To4(), *col5[0])
											assert.Equal(t, col2Data.To4(), *col5[2])
										}
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
