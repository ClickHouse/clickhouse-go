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
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleArray(t *testing.T) {
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
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_array")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array"); assert.NoError(t, err) {
				var (
					col1Data = []string{"A", "b", "c"}
				)
				for i := 0; i < 10; i++ {
					if err := batch.Append(col1Data); !assert.NoError(t, err) {
						return
					}
				}
				if assert.NoError(t, batch.Send()) {
					if rows, err := conn.Query(ctx, "SELECT * FROM test_array"); assert.NoError(t, err) {
						for rows.Next() {
							var (
								col1 []string
							)
							if err := rows.Scan(&col1); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
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

func TestInterfaceArray(t *testing.T) {
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
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_array")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
		require.NoError(t, err)
		var (
			col1Data = []string{"A", "b", "c"}
		)
		for i := 0; i < 10; i++ {
			require.NoError(t, batch.Append(col1Data))
		}
		require.Nil(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT * FROM test_array")
		require.NoError(t, err)
		for rows.Next() {
			var (
				col1 interface{}
			)
			require.NoError(t, rows.Scan(&col1))
			assert.ObjectsAreEqual(col1Data, col1)
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	}
}

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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
			, Col4 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_array")
		}()
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
					col4Data = &[]string{"M", "D"}
				)
				for i := 0; i < 10; i++ {
					if err := batch.Append(col1Data, col2Data, col3Data, col4Data); !assert.NoError(t, err) {
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
								col4 []string
							)
							if err := rows.Scan(&col1, &col2, &col3, &col4); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data, col2)
								assert.Equal(t, col3Data, col3)
								assert.Equal(t, col4Data, &col4)
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
			, Col4 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_array")
		}()
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
				col4Data = &[]string{"M", "D"}

				col1DataColArr [][]string
				col2DataColArr [][][]uint32
				col3DataColArr [][][][]time.Time
				col4DataColArr []*[]string
			)

			for i := 0; i < 10; i++ {
				col1DataColArr = append(col1DataColArr, col1Data)
				col2DataColArr = append(col2DataColArr, col2Data)
				col3DataColArr = append(col3DataColArr, col3Data)
				col4DataColArr = append(col4DataColArr, col4Data)
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
				if err := batch.Column(3).Append(col4DataColArr); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					if rows, err := conn.Query(ctx, "SELECT * FROM test_array"); assert.NoError(t, err) {
						for rows.Next() {
							var (
								col1 []string
								col2 [][]uint32
								col3 [][][]time.Time
								col4 []string
							)
							if err := rows.Scan(&col1, &col2, &col3, &col4); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data, col2)
								assert.Equal(t, col3Data, col3)
								assert.Equal(t, col4Data, &col4)
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
