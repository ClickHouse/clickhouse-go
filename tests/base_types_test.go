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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestUInt8(t *testing.T) {
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
			CREATE TABLE test_uint8 (
				  ID   UInt8
				, Col1 UInt8
				, Col2 Nullable(UInt8)
				, Col3 Array(UInt8)
				, Col4 Array(Nullable(UInt8))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_uint8")
		}()
		type result struct {
			ColID uint8 `ch:"ID"`
			Col1  uint8
			Col2  *uint8
			Col3  []uint8
			Col4  []*uint8
		}
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uint8"); assert.NoError(t, err) {
				data := uint8(42)
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(1), data, &data, []uint8{data}, []*uint8{&data, nil, &data}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(2), data, nil, []uint8{data}, []*uint8{nil, nil, &data}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var (
						result1 result
						result2 result
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_uint8 WHERE ID = $1", 1).ScanStruct(&result1); assert.NoError(t, err) {
						if assert.Equal(t, data, result1.Col1) {
							assert.Equal(t, data, *result1.Col2)
							assert.Equal(t, []uint8{data}, result1.Col3)
							assert.Equal(t, []*uint8{&data, nil, &data}, result1.Col4)
						}
					}
					if err := conn.QueryRow(ctx, "SELECT * FROM test_uint8 WHERE ID = $1", 2).ScanStruct(&result2); assert.NoError(t, err) {
						if assert.Equal(t, data, result2.Col1) {
							if assert.Nil(t, result2.Col2) {
								assert.Equal(t, []uint8{data}, result2.Col3)
								assert.Equal(t, []*uint8{nil, nil, &data}, result2.Col4)
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarUInt8(t *testing.T) {
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_uint8_c (
			  ID   UInt64
			, Col1 UInt8
			, Col2 Nullable(UInt8)
			, Col3 Array(UInt8)
			, Col4 Array(Nullable(UInt8))
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_uint8_c")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uint8_c"); assert.NoError(t, err) {
				var (
					id       []uint64
					col1Data []uint8
					col2Data []*uint8
					col3Data [][]uint8
					col4Data [][]*uint8
				)
				data := uint8(42)
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1Data = append(col1Data, data)
					if i%2 == 0 {
						col2Data = append(col2Data, &data)
					} else {
						col2Data = append(col2Data, nil)
					}
					col3Data = append(col3Data, []uint8{
						data, data, data,
					})
					col4Data = append(col4Data, []*uint8{
						&data, nil, &data,
					})
				}
				{
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
				}
				if assert.NoError(t, batch.Send()) {
					var result struct {
						Col1 uint8
						Col2 *uint8
						Col3 []uint8
						Col4 []*uint8
					}
					if err := conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4 FROM test_uint8_c WHERE ID = $1", 11).ScanStruct(&result); assert.NoError(t, err) {
						if assert.Nil(t, result.Col2) {
							assert.Equal(t, data, result.Col1)
							assert.Equal(t, []uint8{data, data, data}, result.Col3)
							assert.Equal(t, []*uint8{&data, nil, &data}, result.Col4)
						}
					}
				}
			}
		}
	}
}
