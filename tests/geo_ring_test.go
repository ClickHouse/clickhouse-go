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
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestGeoRing(t *testing.T) {
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
		CREATE TABLE test_geo_ring (
			Col1 Ring
			, Col2 Array(Ring)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_ring")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_ring")
	require.NoError(t, err)
	var (
		col1Data = orb.Ring{
			orb.Point{1, 2},
			orb.Point{1, 2},
		}
		col2Data = []orb.Ring{
			orb.Ring{
				orb.Point{1, 2},
				orb.Point{1, 2},
			},
			orb.Ring{
				orb.Point{1, 2},
				orb.Point{1, 2},
			},
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 orb.Ring
		col2 []orb.Ring
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_ring").Scan(&col1, &col2))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
}

func TestGeoRingFlush(t *testing.T) {
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
		CREATE TABLE test_geo_ring_flush (
			  Col1 Ring
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_geo_ring_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_ring_flush")
	require.NoError(t, err)
	vals := [1000]orb.Ring{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Ring{
			orb.Point{1, 2},
			orb.Point{1, 2},
		}
		require.NoError(t, batch.Append(vals[i]))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_ring_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Ring
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

type testGeoRingSerializer struct {
	val orb.Ring
}

func (c testGeoRingSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testGeoRingSerializer) Scan(src any) error {
	if t, ok := src.(orb.Ring); ok {
		*c = testGeoRingSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testGeoRingSerializer", src)
}

func TestGeoRingValuer(t *testing.T) {
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
		CREATE TABLE test_geo_ring_valuer (
			  Col1 Ring
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_geo_ring_valuer")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_ring_valuer")
	require.NoError(t, err)
	vals := [1000]orb.Ring{}
	for i := 0; i < 1000; i++ {
		vals[i] = orb.Ring{
			orb.Point{1, 2},
			orb.Point{1, 2},
		}
		require.NoError(t, batch.Append(testGeoRingSerializer{val: vals[i]}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_geo_ring_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 orb.Ring
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
