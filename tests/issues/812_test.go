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

func Test812(t *testing.T) {
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
	const ddl = `
		CREATE TABLE test_812 (
			Col1 Tuple(name String, age UInt8),
			Col2 Tuple(String, UInt8),
			Col3 Tuple(name String, id String),
			Col4 Array(Tuple(name String, age UInt8))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_812")
	}()
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_812")
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_812")
	require.NoError(t, batch.Append(
		map[string]any{"name": "Clicky McClickHouse Jnr", "age": uint8(20)},
		[]any{"Baby Clicky McClickHouse", uint8(1)},
		map[string]string{"name": "Geoff", "id": "12123"},
		// Col4
		[]any{
			map[string]any{"name": "Clicky McClickHouse Jnr", "age": uint8(20)},
		},
	))
	require.NoError(t, batch.Send())
}
