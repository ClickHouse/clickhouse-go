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

package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue476(t *testing.T) {
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
			CREATE TABLE issue_476 (
				  Col1 Array(LowCardinality(String))
				, Col2 Array(LowCardinality(String))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE issue_476")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_476"); assert.NoError(t, err) {
				if err := batch.Append(
					[]string{"A", "B", "C"},
					[]string{},
				); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var (
						col1 []string
						col2 []string
					)
					if err := conn.QueryRow(ctx, `SELECT * FROM issue_476`).Scan(&col1, &col2); assert.NoError(t, err) {
						assert.Equal(t, []string{"A", "B", "C"}, col1)
						assert.Equal(t, []string{}, col2)
					}
				}
			}
		}
	}
}
