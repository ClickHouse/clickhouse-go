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

func TestDateTime64(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_datetime64 (
				  Col1 DateTime64(3)
				, Col2 DateTime64(9, 'Europe/Moscow')
				, Col3 DateTime64(0, 'Europe/London')
				, Col4 Nullable(DateTime64(3, 'Europe/Moscow'))
				, Col5 Array(DateTime64(3, 'Europe/Moscow'))
				, Col6 Array(Nullable(DateTime64(3, 'Europe/Moscow')))
				, Col7 DateTime64(3) 
				, Col8 DateTime64(6) 
				, Col9 DateTime64(9)
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_datetime64")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Nanosecond)
					datetime3 = time.Now().Truncate(time.Second)
				)
				if err := batch.Append(
					datetime1,
					datetime2,
					datetime3,
					&datetime1,
					[]time.Time{datetime1, datetime1},
					[]*time.Time{&datetime3, nil, &datetime3},
					datetime1.UTC().Format("2006-01-02 15:04:05.999"),
					datetime1.UTC().Format("2006-01-02 15:04:05.999"),
					datetime1.UTC().Format("2006-01-02 15:04:05.999"),
				); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 time.Time
							col2 time.Time
							col3 time.Time
							col4 *time.Time
							col5 []time.Time
							col6 []*time.Time
							col7 time.Time
							col8 time.Time
							col9 time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9); assert.NoError(t, err) {
							assert.Equal(t, datetime1, col1)
							assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
							assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
							if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
								assert.Equal(t, "Europe/London", col3.Location().String())
							}
							assert.Equal(t, datetime1.UnixNano(), col4.UnixNano())
							if assert.Len(t, col5, 2) {
								assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
								assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
							}
							if assert.Len(t, col6, 3) {
								assert.Nil(t, col6[1])
								assert.NotNil(t, col6[0])
								assert.NotNil(t, col6[2])
							}
							assert.Equal(t, datetime1, col7)
							assert.Equal(t, datetime1, col8)
							assert.Equal(t, datetime1, col9)
						}
					}
				}
			}
		}
	}
}

func TestDateTime64AsReference(t *testing.T) {
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
		})
	)

	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 20, 3, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_datetime64 (
				Col1      DateTime64(3)
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_datetime64")
		}()

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				now := time.Now().Unix()
				err := batch.Append(&now)
				assert.NoError(t, err)
				assert.NoError(t, batch.Send())

			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				// batch column
				var col1Data []*int64
				var datetime1 = time.Now().Unix()
				for i := 0; i < 1000; i++ {
					col1Data = append(col1Data, &datetime1)
				}
				if err := batch.Column(0).Append(col1Data); !assert.NoError(t, err) {
					return
				}
				assert.NoError(t, batch.Send())
			}
		}
	}
}

func TestNullableDateTime64(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_datetime64 (
				    Col1      DateTime64(3)
				, Col1_Null Nullable(DateTime64(3))
				, Col2      DateTime64(9, 'Europe/Moscow')
				, Col2_Null Nullable(DateTime64(9, 'Europe/Moscow'))
				, Col3      DateTime64(0, 'Europe/London')
				, Col3_Null Nullable(DateTime64(0, 'Europe/London'))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_datetime64")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Nanosecond)
					datetime3 = time.Now().Truncate(time.Second)
				)
				if err := batch.Append(datetime1, datetime1, datetime2, datetime2, datetime3, datetime3); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1     time.Time
							col1Null *time.Time
							col2     time.Time
							col2Null *time.Time
							col3     time.Time
							col3Null *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
							&col1, &col1Null,
							&col2, &col2Null,
							&col3, &col3Null,
						); assert.NoError(t, err) {
							assert.Equal(t, datetime1, col1)
							assert.Equal(t, datetime1, *col1Null)
							assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
							assert.Equal(t, datetime2.UnixNano(), col2Null.UnixNano())
							assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
							assert.Equal(t, datetime3.UnixNano(), col3Null.UnixNano())
						}
					}
				}
			}
			if err := conn.Exec(ctx, "TRUNCATE TABLE test_datetime64"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Nanosecond)
					datetime3 = time.Now().Truncate(time.Second)
				)
				if err := batch.Append(datetime1, nil, datetime2, nil, datetime3, nil); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1     time.Time
							col1Null *time.Time
							col2     time.Time
							col2Null *time.Time
							col3     time.Time
							col3Null *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
							&col1, &col1Null,
							&col2, &col2Null,
							&col3, &col3Null,
						); assert.NoError(t, err) {
							if assert.Nil(t, col1Null) {
								assert.Equal(t, datetime1, col1)
							}
							if assert.Nil(t, col2Null) {
								if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
									assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())

								}
							}
							if assert.Nil(t, col3Null) {
								if assert.Equal(t, "Europe/London", col3.Location().String()) {
									assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarDateTime64(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TABLE test_datetime64 (
			  ID   UInt64
			, Col1 DateTime64(3)
			, Col2 Nullable(DateTime64(3))
			, Col3 Array(DateTime64(3))
			, Col4 Array(Nullable(DateTime64(3)))
			, Col5 DateTime64(3) 
			, Col6 DateTime64(6) 
			, Col7 DateTime64(9)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_datetime64")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
				var (
					id       []uint64
					col1Data []time.Time
					col2Data []*time.Time
					col3Data [][]time.Time
					col4Data [][]*time.Time
					col5Data []string
					col6Data []string
					col7Data []string
				)
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Second)
				)
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1Data = append(col1Data, datetime1)
					if i%2 == 0 {
						col2Data = append(col2Data, &datetime2)
					} else {
						col2Data = append(col2Data, nil)
					}
					col3Data = append(col3Data, []time.Time{
						datetime1, datetime2, datetime1,
					})
					col4Data = append(col4Data, []*time.Time{
						&datetime2, nil, &datetime1,
					})

					col5Data = append(col5Data, datetime1.UTC().Format("2006-01-02 15:04:05.999"))
					col6Data = append(col6Data, datetime1.UTC().Format("2006-01-02 15:04:05.999"))
					col7Data = append(col7Data, datetime1.UTC().Format("2006-01-02 15:04:05.999"))
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
					if err := batch.Column(5).Append(col5Data); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(6).Append(col6Data); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(7).Append(col7Data); !assert.NoError(t, err) {
						return
					}
				}
				if assert.NoError(t, batch.Send()) {
					var result struct {
						Col1 time.Time
						Col2 *time.Time
						Col3 []time.Time
						Col4 []*time.Time
						Col5 time.Time
						Col6 time.Time
						Col7 time.Time
					}
					if err := conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6, Col7 FROM test_datetime64 WHERE ID = $1", 11).ScanStruct(&result); assert.NoError(t, err) {
						if assert.Nil(t, result.Col2) {
							assert.Equal(t, datetime1, result.Col1)
							assert.Equal(t, []time.Time{datetime1, datetime2, datetime1}, result.Col3)
							assert.Equal(t, []*time.Time{&datetime2, nil, &datetime1}, result.Col4)
							assert.Equal(t, datetime1, result.Col5)
							assert.Equal(t, datetime1, result.Col6)
							assert.Equal(t, datetime1, result.Col7)
						}
					}
				}
			}
		}
	}
}
