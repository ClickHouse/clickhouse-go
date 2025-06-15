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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestAbort(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		ctx := context.Background()
		const ddl = `
		CREATE TABLE test_abort (
			Col1 UInt8
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_abort")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_abort")
		require.NoError(t, err)
		require.NoError(t, batch.Abort())
		if err := batch.Abort(); assert.Error(t, err) {
			assert.Equal(t, clickhouse.ErrBatchAlreadySent, err)
		}
		batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_abort")
		require.NoError(t, err)
		if assert.NoError(t, batch.Append(uint8(1))) && assert.NoError(t, batch.Send()) {
			var col1 uint8
			if err := conn.QueryRow(ctx, "SELECT * FROM test_abort").Scan(&col1); assert.NoError(t, err) {
				assert.Equal(t, uint8(1), col1)
			}
		}
	})
}

func TestBatchClose(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		ctx := context.Background()

		if protocol == clickhouse.HTTP {
			// For HTTP, provide specific column names since we can't parse out the null table function
			ctx = clickhouse.Context(ctx,
				clickhouse.WithColumnNamesAndTypes([]clickhouse.ColumnNameAndType{
					{Name: "x", Type: "UInt64"},
				}))
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO function null('x UInt64') VALUES (1)")
		require.NoError(t, err)
		require.NoError(t, batch.Close())
		require.NoError(t, batch.Close()) // No error on multiple calls

		batch, err = conn.PrepareBatch(ctx, "INSERT INTO function null('x UInt64') VALUES (1)")
		require.NoError(t, err)
		if assert.NoError(t, batch.Append(uint8(1))) && assert.NoError(t, batch.Send()) {
			var col1 uint8
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&col1); assert.NoError(t, err) {
				assert.Equal(t, uint8(1), col1)
			}
		}
	})
}
