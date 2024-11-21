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
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVariant(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":              60,
		"allow_experimental_variant_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Variant(Array(UInt8), Bool, Int64, String)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(42))
	require.NoError(t, batch.Append(chcol.NewVariantWithType("test", "String")))
	require.NoError(t, batch.Append(true))
	require.NoError(t, batch.Append(chcol.NewVariant([]uint8{0xA, 0xB, 0xC}).WithType("Array(UInt8)")))
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Append(84))
	require.NoError(t, batch.Append(chcol.NewVariantWithType("test2", "String")))
	require.NoError(t, batch.Append(true))
	require.NoError(t, batch.Append(chcol.NewVariant([]uint8{0xD, 0xE, 0xF}).WithType("Array(UInt8)")))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c from test_variant")
	require.NoError(t, err)

	var row chcol.Variant

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, int64(42), row.MustInt64())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, "test", row.MustString())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, true, row.MustBool())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, []uint8{0xA, 0xB, 0xC}, row.Any().([]uint8))

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, nil, row.Any())

	var i int64
	require.True(t, rows.Next())
	err = rows.Scan(&i)
	require.NoError(t, err)
	require.Equal(t, int64(84), i)

	var s string
	require.True(t, rows.Next())
	err = rows.Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "test2", s)

	var b bool
	require.True(t, rows.Next())
	err = rows.Scan(&b)
	require.NoError(t, err)
	require.Equal(t, true, b)

	var u8s []uint8
	require.True(t, rows.Next())
	err = rows.Scan(&u8s)
	require.NoError(t, err)
	require.Equal(t, []uint8{0xD, 0xE, 0xF}, u8s)

}
