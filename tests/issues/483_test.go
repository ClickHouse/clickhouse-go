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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue483(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddl = `
		CREATE TABLE issue_483
		(
			example_id UInt8,
			steps Nested(
				  duration UInt8,
				  result Nested(
						duration UInt64,
						error_message Nullable(String),
						status UInt8
					),
				  keyword String
				),
			status UInt8
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_483")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_483 (example_id)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1)))
	require.NoError(t, batch.Send())
	var (
		col1 uint8
		col2 []uint8   // steps.duration
		col3 [][][]any // steps.result
		col4 []string  //  steps.keyword
		col5 uint8
	)
	require.NoError(t, conn.QueryRow(ctx, `SELECT * FROM issue_483`).Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Equal(t, uint8(1), col1)
	assert.Equal(t, []uint8{}, col2)
}
