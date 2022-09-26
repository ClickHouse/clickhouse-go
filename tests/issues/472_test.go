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
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

func TestIssue472(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE issue_472 (
				PodUID               UUID
				, EventType          String
				, ControllerRevision UInt8
				, Timestamp          DateTime
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_472")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_472")
	require.NoError(t, err)
	podUID := uuid.New()
	require.NoError(t, batch.Append(
		podUID,
		"Test",
		uint8(1),
		time.Now(),
	))
	require.NoError(t, batch.Send())
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

	ctx = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}))
	require.NoError(t, conn.Select(ctx, &records, query, podUID, "Test", "", 1))
	t.Log(records)
}
