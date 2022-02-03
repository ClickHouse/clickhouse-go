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

func TestDate(t *testing.T) {
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
			CREATE TABLE test_date (
				  ID   UInt8
				, Col1 Date
				, Col2 Nullable(Date)
				, Col3 Array(Date)
				, Col4 Array(Nullable(Date))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_date")
		}()
		type result struct {
			ColID uint8 `ch:"ID"`
			Col1  time.Time
			Col2  *time.Time
			Col3  []time.Time
			Col4  []*time.Time
		}

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(1), date, &date, []time.Time{date}, []*time.Time{&date, nil, &date}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(2), date, nil, []time.Time{date}, []*time.Time{nil, nil, &date}); !assert.NoError(t, err) {
					return
				}

				if err := batch.Send(); assert.NoError(t, err) {
					var (
						result1 result
						result2 result
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_date WHERE ID = $1", 1).ScanStruct(&result1); assert.NoError(t, err) {
						if assert.Equal(t, date, result1.Col1) {
							assert.Equal(t, "UTC", result1.Col1.Location().String())
							assert.Equal(t, date, *result1.Col2)
							assert.Equal(t, []time.Time{date}, result1.Col3)
							assert.Equal(t, []*time.Time{&date, nil, &date}, result1.Col4)
						}
					}
					if err := conn.QueryRow(ctx, "SELECT * FROM test_date WHERE ID = $1", 2).ScanStruct(&result2); assert.NoError(t, err) {
						if assert.Equal(t, date, result2.Col1) {
							assert.Equal(t, "UTC", result2.Col1.Location().String())
							if assert.Nil(t, result2.Col2) {
								assert.Equal(t, []time.Time{date}, result2.Col3)
								assert.Equal(t, []*time.Time{nil, nil, &date}, result2.Col4)
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableDate(t *testing.T) {
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
			CREATE TABLE test_date (
				  Col1 Date
				, Col2 Nullable(Date)
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_date")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(date, date); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 *time.Time
							col2 *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, date, *col1)
							assert.Equal(t, date, *col2)
						}
					}
				}
			}
			if err := conn.Exec(ctx, "TRUNCATE TABLE test_date"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(date, nil); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 *time.Time
							col2 *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2); assert.NoError(t, err) {
							if assert.Nil(t, col2) {
								assert.Equal(t, date, *col1)
								assert.Equal(t, date.Unix(), col1.Unix())
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarDate(t *testing.T) {
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
		CREATE TABLE test_date (
			  ID   UInt64
			, Col1 Date
			, Col2 Nullable(Date)
			, Col3 Array(Date)
			, Col4 Array(Nullable(Date))
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_date")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
				var (
					id       []uint64
					col1Data []time.Time
					col2Data []*time.Time
					col3Data [][]time.Time
					col4Data [][]*time.Time
				)
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1Data = append(col1Data, date)
					if i%2 == 0 {
						col2Data = append(col2Data, &date)
					} else {
						col2Data = append(col2Data, nil)
					}
					col3Data = append(col3Data, []time.Time{
						date, date, date,
					})
					col4Data = append(col4Data, []*time.Time{
						&date, nil, &date,
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
						Col1 time.Time
						Col2 *time.Time
						Col3 []time.Time
						Col4 []*time.Time
					}
					if err := conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4 FROM test_date WHERE ID = $1", 11).ScanStruct(&result); assert.NoError(t, err) {
						if assert.Nil(t, result.Col2) {
							assert.Equal(t, date, result.Col1)
							assert.Equal(t, []time.Time{date, date, date}, result.Col3)
							assert.Equal(t, []*time.Time{&date, nil, &date}, result.Col4)
						}
					}
				}
			}
		}
	}
}
