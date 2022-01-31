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
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIPv4(t *testing.T) {
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
			//	Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
			CREATE TEMPORARY TABLE test_ipv4 (
				  Col1 IPv4
				, Col2 IPv4
				, Col3 Nullable(IPv4)
				, Col4 Array(IPv4)
				, Col5 Array(Nullable(IPv4))
			)
		`

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv4"); assert.NoError(t, err) {
				var (
					col1Data = net.ParseIP("127.0.0.1")
					col2Data = net.ParseIP("8.8.8.8")
					col3Data = col1Data
					col4Data = []net.IP{col1Data, col2Data}
					col5Data = []*net.IP{&col1Data, nil, &col2Data}
				)
				if err := batch.Append(col1Data, col2Data, &col3Data, &col4Data, &col5Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 net.IP
							col2 net.IP
							col3 *net.IP
							col4 []net.IP
							col5 []*net.IP
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_ipv4").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
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
}

func TestNullableIPv4(t *testing.T) {
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
			CREATE TEMPORARY TABLE test_ipv4 (
				  Col1 Nullable(IPv4)
				, Col2 Nullable(IPv4)
			)
		`

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv4"); assert.NoError(t, err) {
				var (
					col1Data = net.ParseIP("127.0.0.1").To4()
					col2Data = net.ParseIP("8.8.8.8").To4()
				)
				if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 *net.IP
							col2 *net.IP
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_ipv4").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, col1Data, *col1)
							assert.Equal(t, col2Data, *col2)
						}
					}
				}
			}
		}

		if err := conn.Exec(ctx, "TRUNCATE TABLE test_ipv4"); !assert.NoError(t, err) {
			return
		}
		if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv4"); assert.NoError(t, err) {
			var col1Data = net.ParseIP("1.1.1.1").To4()
			if err := batch.Append(col1Data, nil); assert.NoError(t, err) {
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *net.IP
						col2 *net.IP
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_ipv4").Scan(&col1, &col2); assert.NoError(t, err) {
						if assert.Nil(t, col2) {
							assert.Equal(t, col1Data, *col1)
						}
					}
				}
			}
		}
	}
}

func TestColumnarIPv4(t *testing.T) {
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
			CREATE TEMPORARY TABLE test_ipv4 (
				  Col1 IPv4
				, Col2 IPv4
				, Col3 Nullable(IPv4)
			)
		`

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_ipv4"); assert.NoError(t, err) {
				var (
					col1Data []*net.IP
					col2Data []*net.IP
					col3Data []*net.IP
					v1, v2   = net.ParseIP("1.1.1.1"), net.ParseIP("8.8.8.8")
				)
				col1Data = append(col1Data, &v1)
				col2Data = append(col2Data, &v2)
				col3Data = append(col3Data, nil)
				{
					batch.Column(0).Append(col1Data)
					batch.Column(1).Append(col2Data)
					batch.Column(2).Append(col3Data)
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *net.IP
						col2 *net.IP
						col3 *net.IP
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_ipv4").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						if assert.Nil(t, col3) {
							assert.Equal(t, v1.To4(), *col1)
							assert.Equal(t, v2.To4(), *col2)
						}
					}
				}
			}
		}
	}
}
