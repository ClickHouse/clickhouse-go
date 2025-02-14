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
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestZSTDCompression(t *testing.T) {
	CompressionTest(t, compress.LevelZero, clickhouse.CompressionZSTD)
}

func TestLZ4Compression(t *testing.T) {
	CompressionTest(t, compress.Level(3), clickhouse.CompressionLZ4)
}

func TestLZ4HCCompression(t *testing.T) {
	CompressionTest(t, compress.LevelLZ4HCDefault, clickhouse.CompressionLZ4HC)
}

func TestNoCompression(t *testing.T) {
	CompressionTest(t, compress.LevelZero, clickhouse.CompressionNone)
}

func CompressionTest(t *testing.T, level compress.Level, method clickhouse.CompressionMethod) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: method,
		Level:  int(level),
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		col1Data = []string{"A", "b", "c"}
	)
	for i := 0; i < 100; i++ {
		require.NoError(t, batch.Append(col1Data))
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 []string
		)
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, col1Data, col1)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}
