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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue584(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_584"))
	}()

	const ddl = `
	CREATE TABLE issue_584 (
		Col1 Map(String, String)
	) Engine MergeTree() ORDER BY tuple()
	`
	require.NoError(t, conn.Exec(context.Background(), "DROP TABLE IF EXISTS issue_584"))
	require.NoError(t, conn.Exec(context.Background(), ddl))
	require.NoError(t, conn.Exec(context.Background(), "INSERT INTO issue_584 values($1)", map[string]string{
		"key": "value",
	}))
	var event map[string]string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT * FROM issue_584").Scan(&event))
	assert.Equal(t, map[string]string{
		"key": "value",
	}, event)
}

func TestIssue584Complex(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_584_complex"))
	}()

	const ddl = `
	CREATE TABLE issue_584_complex (
		Col1 Map(String, Map(UInt8, Array(UInt8)))
	) Engine MergeTree() ORDER BY tuple()
	`
	require.NoError(t, conn.Exec(context.Background(), "DROP TABLE IF EXISTS issue_584_complex"))
	require.NoError(t, conn.Exec(context.Background(), ddl))
	col1 := map[string]map[uint8][]uint8{
		"a": {
			100: []uint8{1, 2, 3, 4},
			99:  []uint8{5, 6, 7, 8},
		},
		"d": {
			98: []uint8{10, 11, 12, 13},
		},
	}
	require.NoError(t, conn.Exec(context.Background(), "INSERT INTO issue_584_complex values($1)", col1))
	var event map[string]map[uint8][]uint8
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT * FROM issue_584_complex").Scan(&event))
	assert.Equal(t, col1, event)

}
