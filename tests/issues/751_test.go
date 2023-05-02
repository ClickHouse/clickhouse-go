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
	"database/sql"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue751(t *testing.T) {
	conn, err := clickhouse_tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	conn.Exec(ctx, "DROP TABLE IF EXISTS issue_751")

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE issue_751 (
				Col1 Nullable(String),
				Col2 String,
				Col3 Nullable(Int8),
				Col4 Nullable(Int64),
				Col5 LowCardinality(Nullable(String))
			)
			Engine MergeTree() ORDER BY tuple()
		`))
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_751")
	}()
	type Example struct {
		Col1 *string
		Col2 string
		Col3 *int8
		Col4 *int64
		Col5 *string
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_751")
	require.NoError(t, err)
	example := Example{}
	require.NoError(t, batch.AppendStruct(&example))
	batch.Append(example.Col1, example.Col2, example.Col3, example.Col4, example.Col5)
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT * FROM issue_751")
	require.NoError(t, err)
	c := 0
	for rows.Next() {
		var (
			col1 *string
			col2 string
			col3 *int8
			col4 sql.NullInt64
			col5 *string
		)
		require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4, &col5))
		assert.Nil(t, col1)
		assert.Equal(t, "", col2)
		assert.Nil(t, col3)
		assert.Equal(t, sql.NullInt64{
			Int64: 0,
			Valid: false,
		}, col4)
		assert.Nil(t, col5)
		c++
	}
	assert.Equal(t, 2, c)
}
