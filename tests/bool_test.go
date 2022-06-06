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

func TestBool(t *testing.T) {
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
			conn.Exec(ctx, "DROP TABLE test_bool")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool"); assert.NoError(t, err) {
				var val bool
				if err := batch.Append(true, false, []bool{true, false, true}, nil, []*bool{&val, nil, &val}); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 bool
							col2 bool
							col3 []bool
							col4 *bool
							col5 []*bool
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_bool").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
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
}

func TestColumnarBool(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 12, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_bool (
				  ID   UInt64
				, Col1 Bool
				, Col2 Bool
				, Col3 Array(Bool)
				, Col4 Nullable(Bool)
				, Col5 Array(Nullable(Bool))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_bool")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			val := true
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool"); assert.NoError(t, err) {
				var (
					id   []uint64
					col1 []bool
					col2 []bool
					col3 [][]bool
					col4 []*bool
					col5 [][]*bool
				)
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1 = append(col1, true)
					col2 = append(col2, false)
					col3 = append(col3, []bool{true, false, true})
					col4 = append(col4, nil)
					col5 = append(col5, []*bool{&val, nil, &val})
				}
				{
					if err := batch.Column(0).Append(id); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(1).Append(col1); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(2).Append(col2); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(3).Append(col3); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(4).Append(col4); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(5).Append(col5); !assert.NoError(t, err) {
						return
					}
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							id   uint64
							col1 bool
							col2 bool
							col3 []bool
							col4 *bool
							col5 []*bool
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_bool WHERE ID = $1", 42).Scan(&id, &col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
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
}
