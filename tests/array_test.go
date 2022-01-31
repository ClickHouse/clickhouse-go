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

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestArray(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array"); assert.NoError(t, err) {
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
					if err := batch.Append(col1Data, col2Data, col3Data); !assert.NoError(t, err) {
						return
					}
				}
				if assert.NoError(t, batch.Send()) {
					if rows, err := conn.Query(ctx, "SELECT * FROM test_array"); assert.NoError(t, err) {
						for rows.Next() {
							var (
								col1 []string
								col2 [][]uint32
								col3 [][][]time.Time
							)
							if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data, col2)
								assert.Equal(t, col3Data, col3)
							}
						}
						if assert.NoError(t, rows.Close()) {
							assert.NoError(t, rows.Err())
						}
					}
				}
			}
		}
	}
}

func TestColumnarArray(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
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

				col1DataColArr [][]string
				col2DataColArr [][][]uint32
				col3DataColArr [][][][]time.Time
			)

			for i := 0; i < 10; i++ {
				col1DataColArr = append(col1DataColArr, col1Data)
				col2DataColArr = append(col2DataColArr, col2Data)
				col3DataColArr = append(col3DataColArr, col3Data)
			}

			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array"); assert.NoError(t, err) {
				if err := batch.Column(0).Append(col1DataColArr); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(1).Append(col2DataColArr); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(2).Append(col3DataColArr); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					if rows, err := conn.Query(ctx, "SELECT * FROM test_array"); assert.NoError(t, err) {
						for rows.Next() {
							var (
								col1 []string
								col2 [][]uint32
								col3 [][][]time.Time
							)
							if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data, col2)
								assert.Equal(t, col3Data, col3)
							}
						}
						if assert.NoError(t, rows.Close()) {
							assert.NoError(t, rows.Err())
						}
					}
				}
			}
		}
	}
}
