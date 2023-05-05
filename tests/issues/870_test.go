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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test870(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
			"flatten_nested":     0,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}

	ctx := context.Background()
	require.NoError(t, err)
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	ddl := fmt.Sprintf("CREATE TABLE `%s`.`test_870` (Col1 String, Col2 Int64) Engine MergeTree() ORDER BY tuple()", env.Database)
	defer func() {
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`test_870`", env.Database))
	}()
	conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`test_870`", env.Database))
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO `%s`.`test_870` (`Col1`, `Col2`)", env.Database))
	require.NoError(t, err)
	values := []struct {
		col1 string
		col2 int64
	}{
		{"foo", 5},
		{"foo", 10},
	}
	for _, v := range values {
		require.NoError(t, batch.Append(
			v.col1,
			v.col2,
		))
	}
	require.NoError(t, batch.Send())

	queryCtx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
		"groupBy":   "Col1",
		"stringVal": "lorem ipsum",
	}))

	row := conn.QueryRow(queryCtx, "SELECT {groupBy:Identifier} as groupBy, SUM(Col2), {stringVal:String} FROM test_870 GROUP BY {groupBy:Identifier}")
	assert.NoError(t, row.Err())

	var groupBy string
	var sum int64
	var actualStringFromParam string

	assert.NoError(t, row.Scan(&groupBy, &sum, &actualStringFromParam))
	assert.Equal(t, "foo", groupBy)
	assert.Equal(t, int64(15), sum)
	assert.Equal(t, "lorem ipsum", actualStringFromParam)
}
