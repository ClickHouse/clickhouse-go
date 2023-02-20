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

func Test904(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
			"flatten_nested":     0,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)

	ctx := context.Background()
	require.NoError(t, err)
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	ddl := fmt.Sprintf("CREATE TABLE `%s`.`test_904` (Col1 FixedString(6)) Engine MergeTree() ORDER BY tuple()", env.Database)
	defer func() {
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`test_904`", env.Database))
	}()
	require.NoError(t, conn.Exec(ctx, ddl))

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO `%s`.`test_904` (Col1)", env.Database))
	require.NoError(t, err)
	require.NoError(t, batch.Append("foo"))
	require.NoError(t, batch.Send())

	var col1 string
	row := conn.QueryRow(ctx, fmt.Sprintf("SELECT Col1 FROM `%s`.`test_904`", env.Database))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&col1))

	assert.Equal(t, "foo"+string([]byte{0, 0, 0}), col1)
}
