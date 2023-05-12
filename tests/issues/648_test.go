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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue648(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS issue_648")
	require.NoError(t, err)
	require.NoError(t, conn.Exec(
		context.Background(),
		`CREATE TABLE issue_648(
				id Int64,
				arr Array(UInt8), 
				map Map(String, UInt8), 
				mul_arr Array(Array(UInt8)), 
				map_arr Map(UInt8, Array(UInt8)), 
				map_map_arr Map(String, Map(String, Array(UInt8))))
			ENGINE = MergeTree
			ORDER BY (id)`,
	))
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_648"))
	}()
	ctx := context.Background()
	require.NoError(t, err)
	for i := uint8(0); i < 10; i++ {
		require.NoError(t, conn.Exec(ctx, "INSERT INTO issue_648 VALUES(?, ?, ?, ?, ?, ?)",
			int64(i),
			clickhouse.ArraySet{i, i + 1, i + 2},
			map[string]uint8{string([]byte{'A' + i}): i},
			clickhouse.ArraySet{clickhouse.ArraySet{i, i + 1}, clickhouse.ArraySet{i + 1, i + 2}},
			map[uint8][]uint8{i: {i + 1, i + 2}, i + 1: {i + 2, i + 3}},
			map[string]map[string][]uint8{string([]byte{'A' + i}): {string([]byte{'A' + i}): {i + 1, i + 2}}},
		))
	}
	// update array
	require.NoError(t, conn.Exec(ctx, "ALTER TABLE issue_648 UPDATE arr = ? where id = ?", clickhouse.ArraySet{1, 1}, 0))
	// update map
	require.NoError(t, conn.Exec(ctx, "ALTER TABLE issue_648 UPDATE map = ? where id = ?", map[string]uint8{"a": 1}, 0))
	// update mul_array
	require.NoError(t, conn.Exec(ctx, "ALTER TABLE issue_648 UPDATE mul_arr = ? where id = ?", clickhouse.ArraySet{clickhouse.ArraySet{1, 2}, clickhouse.ArraySet{2, 3}}, 0))
	// update map_arr
	require.NoError(t, conn.Exec(ctx, "ALTER TABLE issue_648 UPDATE map_arr = ? where id = ?", map[uint8][]uint8{1: {1, 2, 3}}, 0))
	// update map_map_arr
	require.NoError(t, conn.Exec(ctx, "ALTER TABLE issue_648 UPDATE map_map_arr = ? where id = ?", map[string]map[string][]uint8{"A": {"B": {1, 2}}}, 0))
}
