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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestIssue472(t *testing.T) {
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
			CREATE TABLE issue_472 (
				PodUID               UUID
				, EventType          String
				, ControllerRevision UInt8
				, Timestamp          DateTime
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE issue_472")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_472"); assert.NoError(t, err) {
				podUID := uuid.New()
				if err := batch.Append(
					podUID,
					"Test",
					uint8(1),
					time.Now(),
				); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var records []struct {
						Timestamp time.Time
					}
					const query = `
							SELECT
								Timestamp
							FROM issue_472
							WHERE PodUID = $1
								AND (EventType = $2 or EventType = $3)
								AND ControllerRevision = $4 LIMIT 1`

					ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
						"max_block_size": 10,
					}))
					if err := conn.Select(ctx, &records, query, podUID, "Test", "", 1); assert.NoError(t, err) {
						t.Log(records)
					}
				}
			}
		}
	}
}
