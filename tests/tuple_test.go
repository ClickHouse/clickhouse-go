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
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestTuple(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 9, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TABLE test_tuple (
			  Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime)
			, Col3 Tuple(name1 DateTime, name2 FixedString(2), name3 Map(String, String))
			, Col4 Array(Array( Tuple(String, Int64) ))
			, Col5 Tuple(LowCardinality(String),           Array(LowCardinality(String)))
			, Col6 Tuple(LowCardinality(Nullable(String)), Array(LowCardinality(Nullable(String))))
			, Col7 Tuple(String, Int64)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_tuple")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple"); assert.NoError(t, err) {
				var (
					col1Data = []interface{}{"A", int64(42)}
					col2Data = []interface{}{"B", int8(1), testDate.Truncate(time.Second)}
					col3Data = map[string]interface{}{
						"name1": testDate.Truncate(time.Second),
						"name2": "CH",
						"name3": map[string]string{
							"key": "value",
						},
					}
					col4Data = [][][]interface{}{
						[][]interface{}{
							[]interface{}{"Hi", int64(42)},
						},
					}
					col5Data = []interface{}{
						"LCString",
						[]string{"A", "B", "C"},
					}
					str      = "LCString"
					col6Data = []interface{}{
						&str,
						[]*string{&str, nil, &str},
					}
					col7Data = &[]interface{}{"C", int64(42)}
				)
				if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 []interface{}
							col2 []interface{}
							// col3 is a named tuple - we can use map
							col3 map[string]interface{}
							col4 [][][]interface{}
							col5 []interface{}
							col6 []interface{}
							col7 []interface{}
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.JSONEq(t, `{"name1":"2022-05-25T17:20:57+01:00","name2":"CH","name3":{"key":"value"}}`, toJson(col3))
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, col5Data, col5)
							assert.Equal(t, col6Data, col6)
							assert.Equal(t, col7Data, &col7)
						}
					}
				}
			}
		}
	}
}
func TestColumnarTuple(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 9, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TABLE test_tuple (
			  ID   UInt64
			, Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime)
			, Col3 Tuple(DateTime, FixedString(2), Map(String, String))
			, Col4 Tuple(String, Int64)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_tuple")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple"); assert.NoError(t, err) {
				var (
					id        []uint64
					col1Data  = [][]interface{}{}
					col2Data  = [][]interface{}{}
					col3Data  = [][]interface{}{}
					col4Data  = []*[]interface{}{}
					timestamp = time.Now().Truncate(time.Second)
				)
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1Data = append(col1Data, []interface{}{
						fmt.Sprintf("A_%d", i), int64(i),
					})
					col2Data = append(col2Data, []interface{}{
						fmt.Sprintf("B_%d", i), int8(1), timestamp,
					})
					col3Data = append(col3Data, []interface{}{
						timestamp, "CH", map[string]string{
							"key": "value",
						},
					})
					col4Data = append(col4Data, &[]interface{}{
						fmt.Sprintf("C_%d", i), int64(i),
					})
				}
				if err := batch.Column(0).Append(id); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(1).Append(col1Data); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(2).Append(col2Data); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(3).Append(col3Data); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(4).Append(col4Data); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						id       uint64
						col1     []interface{}
						col2     []interface{}
						col3     []interface{}
						col4     []interface{}
						col1Data = []interface{}{
							"A_542", int64(542),
						}
						col2Data = []interface{}{
							"B_542", int8(1), timestamp,
						}
						col3Data = []interface{}{
							timestamp, "CH", map[string]string{
								"key": "value",
							},
						}
						col4Data = &[]interface{}{
							"C_542", int64(542),
						}
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_tuple WHERE ID = $1", 542).Scan(&id, &col1, &col2, &col3, &col4); assert.NoError(t, err) {
						assert.Equal(t, col1Data, col1)
						assert.Equal(t, col2Data, col2)
						assert.Equal(t, col3Data, col3)
						assert.Equal(t, col4Data, &col4)
					}
				}
			}
		}
	}
}
