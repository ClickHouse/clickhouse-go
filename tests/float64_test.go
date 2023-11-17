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
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFloat64(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_float64 (
				  Col1 Float64
				, Col2 Float64                   
			) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_float64"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_float64 (Col1, Col2)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(1.1, 2.1))
	require.NoError(t, batch.Send())

	row := conn.QueryRow(ctx, "SELECT Col1, Col2 from test_float64")
	require.NoError(t, err)

	var (
		col1 float64
		col2 float64
	)
	require.NoError(t, row.Scan(&col1, &col2))
	require.Equal(t, float64(1.1), col1)
	require.Equal(t, float64(2.1), col2)
}

type testFloat64Serializer struct {
	val float64
}

func (c testFloat64Serializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testFloat64Serializer) Scan(src any) error {
	if t, ok := src.(float64); ok {
		*c = testFloat64Serializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testFloat64Serializer", src)
}

func TestFloat64Valuer(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_float64_valuer (
				  Col1 Float64
				, Col2 Float64                   
			) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_float64_valuer"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_float64_valuer (Col1, Col2)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(testFloat64Serializer{val: 1.1}, testFloat64Serializer{val: 2.1}))
	require.NoError(t, batch.Send())

	row := conn.QueryRow(ctx, "SELECT Col1, Col2 from test_float64_valuer")
	require.NoError(t, err)

	var (
		col1 float64
		col2 float64
	)
	require.NoError(t, row.Scan(&col1, &col2))
	require.Equal(t, float64(1.1), col1)
	require.Equal(t, float64(2.1), col2)
}
