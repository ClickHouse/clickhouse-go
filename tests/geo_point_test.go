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
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestGeoPoint(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_geo_point (
			Col1 Point
			, Col2 Array(Point)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_point")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_point")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		orb.Point{11, 22},
		[]orb.Point{
			{1, 2},
			{3, 4},
		},
	))
	require.NoError(t, batch.Send())
	var (
		col1 orb.Point
		col2 []orb.Point
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_point").Scan(&col1, &col2))
	assert.Equal(t, orb.Point{11, 22}, col1)
	assert.Equal(t, []orb.Point{
		{1, 2},
		{3, 4},
	}, col2)
}

func TestGeoPointFlush(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_geo_point_flush (
			  Col1 Point
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_point_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_point_flush")
	require.NoError(t, err)
	vals := [1000]orb.Point{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Point{rand.Float64(), rand.Float64()}
		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_point_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Point
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

type testPointSerializer struct {
	val orb.Point
}

func (c testPointSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testPointSerializer) Scan(src any) error {
	if t, ok := src.(orb.Point); ok {
		*c = testPointSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testPointSerializer", src)
}

func TestGeoPointValuer(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_geo_point_flush (
			  Col1 Point
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_point_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_point_flush")
	require.NoError(t, err)
	vals := [1000]orb.Point{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Point{rand.Float64(), rand.Float64()}
		require.NoError(t, batch.Append(testPointSerializer{val: vals[i]}))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_point_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Point
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
