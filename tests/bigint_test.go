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
	"math/big"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestBigInt(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TEMPORARY TABLE test_bigint (
			  Col1 Int128
			, Col2 Array(Int128)
			, Col3 Int256
			, Col4 Array(Int256)
			, Col5 UInt256
			, Col6 Array(UInt256)
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bigint"); assert.NoError(t, err) {
				var (
					col1Data = big.NewInt(128)
					col2Data = []*big.Int{
						big.NewInt(-128),
						big.NewInt(128128),
						big.NewInt(128128128),
					}
					col3Data = big.NewInt(256)
					col4Data = []*big.Int{
						big.NewInt(256),
						big.NewInt(256256),
						big.NewInt(256256256256),
					}
					col5Data = big.NewInt(256)
					col6Data = []*big.Int{
						big.NewInt(256),
						big.NewInt(256256),
						big.NewInt(256256256256),
					}
				)
				if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 big.Int
							col2 []*big.Int
							col3 big.Int
							col4 []*big.Int
							col5 big.Int
							col6 []*big.Int
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_bigint").Scan(&col1, &col2, &col3, &col4, &col5, &col6); assert.NoError(t, err) {
							assert.Equal(t, *col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, *col3Data, col3)
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, *col5Data, col5)
							assert.Equal(t, col6Data, col6)
						}
					}
				}
			}
		}
	}
}

func TestNullableBigInt(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TEMPORARY TABLE test_nullable_bigint (
			  Col1 Nullable(Int128)
			, Col2 Array(Nullable(Int128))
			, Col3 Nullable(Int256)
			, Col4 Array(Nullable(Int256))
			, Col5 Nullable(UInt256)
			, Col6 Array(Nullable(UInt256))
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nullable_bigint"); assert.NoError(t, err) {
				var (
					col1Data = big.NewInt(128)
					col2Data = []*big.Int{
						big.NewInt(-128),
						big.NewInt(128128),
						big.NewInt(128128128),
					}
					col3Data = big.NewInt(256)
					col4Data = []*big.Int{
						big.NewInt(256),
						nil,
						big.NewInt(256256256256),
					}
					col5Data = big.NewInt(256)
					col6Data = []*big.Int{
						big.NewInt(256),
						big.NewInt(256256),
						big.NewInt(256256256256),
					}
				)
				if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 big.Int
							col2 []*big.Int
							col3 big.Int
							col4 []*big.Int
							col5 big.Int
							col6 []*big.Int
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_nullable_bigint").Scan(&col1, &col2, &col3, &col4, &col5, &col6); assert.NoError(t, err) {
							assert.Equal(t, *col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, *col3Data, col3)
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, *col5Data, col5)
							assert.Equal(t, col6Data, col6)
						}
					}
				}
			}
		}
	}
}
