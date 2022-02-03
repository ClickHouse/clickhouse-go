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

func TestIssue406(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 9); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE issue_406 (
				Col1 Tuple(Array(Int32), Array(Int32))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE issue_406")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_406"); assert.NoError(t, err) {
				if err := batch.Append(
					[]interface{}{
						[]int32{1, 2, 3, 4, 5},
						[]int32{5, 1, 2, 3, 4},
					},
				); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var col1 []interface{}
						if err := conn.QueryRow(ctx, "SELECT * FROM issue_406").Scan(&col1); assert.NoError(t, err) {
							assert.Equal(t, []interface{}{
								[]int32{1, 2, 3, 4, 5},
								[]int32{5, 1, 2, 3, 4},
							}, col1)
						}
					}
				}
			}
		}
	}
}
