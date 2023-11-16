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

func TestGeoPolygon(t *testing.T) {
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
		CREATE TABLE test_geo_polygon (
			  Col1 Polygon
			, Col2 Array(Polygon)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_geo_polygon")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_polygon")
	require.NoError(t, err)
	var (
		col1Data = orb.Polygon{
			orb.Ring{
				orb.Point{1, 2},
				orb.Point{12, 2},
			},
			orb.Ring{
				orb.Point{11, 2},
				orb.Point{1, 12},
			},
		}
		col2Data = []orb.Polygon{
			[]orb.Ring{
				orb.Ring{
					orb.Point{1, 2},
					orb.Point{1, 22},
				},
				orb.Ring{
					orb.Point{1, 23},
					orb.Point{12, 2},
				},
			},
			[]orb.Ring{
				orb.Ring{
					orb.Point{21, 2},
					orb.Point{1, 222},
				},
				orb.Ring{
					orb.Point{21, 23},
					orb.Point{12, 22},
				},
			},
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 orb.Polygon
		col2 []orb.Polygon
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_polygon").Scan(&col1, &col2))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
}

func TestGeoPolygonFlush(t *testing.T) {
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
		CREATE TABLE test_geo_polygon_flush (
			  Col1 Polygon
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_geo_polygon_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_polygon_flush")
	require.NoError(t, err)
	vals := [1000]orb.Polygon{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Polygon{
			orb.Ring{
				orb.Point{rand.Float64(), rand.Float64()},
				orb.Point{rand.Float64(), rand.Float64()},
			},
			orb.Ring{
				orb.Point{rand.Float64(), rand.Float64()},
				orb.Point{rand.Float64(), rand.Float64()},
			},
		}
		require.NoError(t, batch.Append(vals[i]))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_polygon_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Polygon
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

type testPolygonSerializer struct {
	val orb.Polygon
}

func (c testPolygonSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testPolygonSerializer) Scan(src any) error {
	if t, ok := src.(orb.Polygon); ok {
		*c = testPolygonSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testPolygonSerializer", src)
}

func TestGeoPolygonValuer(t *testing.T) {
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
		CREATE TABLE test_geo_polygon_flush (
			  Col1 Polygon
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_geo_polygon_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_polygon_flush")
	require.NoError(t, err)
	vals := [1000]orb.Polygon{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Polygon{
			orb.Ring{
				orb.Point{rand.Float64(), rand.Float64()},
				orb.Point{rand.Float64(), rand.Float64()},
			},
			orb.Ring{
				orb.Point{rand.Float64(), rand.Float64()},
				orb.Point{rand.Float64(), rand.Float64()},
			},
		}
		require.NoError(t, batch.Append(testPolygonSerializer{val: vals[i]}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_polygon_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Polygon
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
