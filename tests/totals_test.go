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

func TestWithTotals(t *testing.T) {
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
		const query = `
		SELECT
			number AS n
			, COUNT()
		FROM (
			SELECT number FROM system.numbers LIMIT 100
		) GROUP BY n WITH TOTALS
		`
		if rows, err := conn.Query(ctx, query); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				count++
				var (
					n uint64
					c uint64
				)
				if !assert.NoError(t, rows.Scan(&n, &c)) {
					return
				}
			}
			if assert.Equal(t, 100, count) {
				var (
					n, totals uint64
				)
				if assert.NoError(t, rows.Totals(&n, &totals)) {
					assert.Equal(t, uint64(0), n)
					assert.Equal(t, uint64(100), totals)
				}
			}
		}
	}
}
