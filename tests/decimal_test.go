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
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestDecimal(t *testing.T) {
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
			Settings: clickhouse.Settings{
				"allow_experimental_bigint_types": 1,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 1); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TEMPORARY TABLE test_decimal (
				Col1 Decimal32(5)
				, Col2 Decimal(18,5)
				, Col3 Decimal(15,3)
				, Col4 Decimal128(5)
				, Col5 Decimal256(5)
			)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal"); assert.NoError(t, err) {
				if err := batch.Append(
					decimal.New(25, 0),
					decimal.New(30, 0),
					decimal.New(35, 0),
					decimal.New(135, 0),
					decimal.New(256, 0),
				); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 decimal.Decimal
						col2 decimal.Decimal
						col3 decimal.Decimal
						col4 decimal.Decimal
						col5 decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_decimal").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
						assert.True(t, decimal.New(25, 0).Equal(col1))
						assert.True(t, decimal.New(30, 0).Equal(col2))
						assert.True(t, decimal.New(35, 0).Equal(col3))
						assert.True(t, decimal.New(135, 0).Equal(col4))
						assert.True(t, decimal.New(256, 0).Equal(col5))
					}
				}
			}
		}
	}
}

func TestNullableDecimal(t *testing.T) {
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
			Settings: clickhouse.Settings{
				"allow_experimental_bigint_types": 1,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 1); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TEMPORARY TABLE test_decimal (
			  Col1 Nullable(Decimal32(5))
			, Col2 Nullable(Decimal(18,5))
			, Col3 Nullable(Decimal(15,3))
		)
		`

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal"); assert.NoError(t, err) {
				if err := batch.Append(decimal.New(25, 0), decimal.New(30, 0), decimal.New(35, 0)); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *decimal.Decimal
						col2 *decimal.Decimal
						col3 *decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_decimal").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						assert.True(t, decimal.New(25, 0).Equal(*col1))
						assert.True(t, decimal.New(30, 0).Equal(*col2))
						assert.True(t, decimal.New(35, 0).Equal(*col3))
					}
				}
			}

			if err := conn.Exec(ctx, "TRUNCATE TABLE test_decimal"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal"); assert.NoError(t, err) {
				if err := batch.Append(decimal.New(25, 0), nil, decimal.New(35, 0)); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *decimal.Decimal
						col2 *decimal.Decimal
						col3 *decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_decimal").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						if assert.Nil(t, col2) {
							assert.True(t, decimal.New(25, 0).Equal(*col1))
							assert.True(t, decimal.New(35, 0).Equal(*col3))
						}
					}
				}
			}
		}
	}
}
