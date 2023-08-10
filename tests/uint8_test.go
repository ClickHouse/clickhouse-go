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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoolUInt8(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS issue_1050 (
				  Col1 UInt8
				, Col2 UInt8                   
			) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1050"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_1050 (Col1, Col2)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(true, false))
	require.NoError(t, batch.Send())

	row := conn.QueryRow(ctx, "SELECT Col1, Col2 from issue_1050")
	require.NoError(t, err)

	var (
		col1 bool
		col2 bool
	)
	require.NoError(t, row.Scan(&col1, &col2))
	require.True(t, col1)
	require.False(t, col2)
}
