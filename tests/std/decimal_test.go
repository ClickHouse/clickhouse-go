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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestStdDecimal(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := checkMinServerVersion(conn, 21, 1, 0); err != nil {
					t.Skip(err.Error())
					return
				}
				const ddl = `
			CREATE TABLE test_decimal (
				Col1 Decimal32(5)
				, Col2 Decimal(18,5)
				, Col3 Nullable(Decimal(15,3))
				, Col4 Array(Decimal(15,3))
			) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_decimal")
				}()
				if _, err := conn.Exec(ddl); assert.NoError(t, err) {
					scope, err := conn.Begin()
					if !assert.NoError(t, err) {
						return
					}
					if batch, err := scope.Prepare("INSERT INTO test_decimal"); assert.NoError(t, err) {
						if _, err := batch.Exec(
							decimal.New(25, 0),
							decimal.New(30, 0),
							decimal.New(35, 0),
							[]decimal.Decimal{
								decimal.New(25, 0),
								decimal.New(30, 0),
								decimal.New(35, 0),
							},
						); !assert.NoError(t, err) {
							return
						}
						if assert.NoError(t, scope.Commit()) {
							var (
								col1 decimal.Decimal
								col2 decimal.Decimal
								col3 decimal.Decimal
								col4 []decimal.Decimal
							)
							if rows, err := conn.Query("SELECT * FROM test_decimal"); assert.NoError(t, err) {
								if columnTypes, err := rows.ColumnTypes(); assert.NoError(t, err) {
									for i, column := range columnTypes {
										switch i {
										case 0:
											nullable, nullableOk := column.Nullable()
											assert.False(t, nullable)
											assert.True(t, nullableOk)

											precision, scale, ok := column.DecimalSize()
											assert.Equal(t, int64(5), scale)
											assert.Equal(t, int64(9), precision)
											assert.True(t, ok)
										case 1:
											nullable, nullableOk := column.Nullable()
											assert.False(t, nullable)
											assert.True(t, nullableOk)

											precision, scale, ok := column.DecimalSize()
											assert.Equal(t, int64(5), scale)
											assert.Equal(t, int64(18), precision)
											assert.True(t, ok)
										case 2:
											nullable, nullableOk := column.Nullable()
											assert.True(t, nullable)
											assert.True(t, nullableOk)

											precision, scale, ok := column.DecimalSize()
											assert.Equal(t, int64(3), scale)
											assert.Equal(t, int64(15), precision)
											assert.True(t, ok)
										case 3:
											nullable, nullableOk := column.Nullable()
											assert.False(t, nullable)
											assert.True(t, nullableOk)

											precision, scale, ok := column.DecimalSize()
											assert.Equal(t, int64(3), scale)
											assert.Equal(t, int64(15), precision)
											assert.True(t, ok)
										}
									}
								}
								for rows.Next() {
									if err := rows.Scan(&col1, &col2, &col3, &col4); assert.NoError(t, err) {
										assert.True(t, decimal.New(25, 0).Equal(col1))
										assert.True(t, decimal.New(30, 0).Equal(col2))
										assert.True(t, decimal.New(35, 0).Equal(col3))
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
