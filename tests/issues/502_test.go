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

func TestIssue502(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddl = `
		CREATE TABLE issue_502
		(
			  Part UInt8
			, Col1 UInt8
			, Col2 UInt8
		) Engine MergeTree
			ORDER BY Part
			PARTITION BY (Part)
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_502")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_502")
	require.NoError(t, err)
	for part := 0; part < 10; part++ {
		require.NoError(t, batch.Append(uint8(part), uint8(part)+10, uint8(part)+20))
	}
	require.NoError(t, batch.Send())
	var result []struct {
		Part uint8
		Col1 uint8
		Col2 uint8
	}
	require.NoError(t, conn.Select(ctx, &result, `SELECT * FROM issue_502`))
	require.Len(t, result, 10)
	for _, v := range result {
		assert.Equal(t, uint8(v.Part)+10, v.Col1)
		assert.Equal(t, uint8(v.Part)+20, v.Col2)
	}
}
