package tests

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestGeoLineString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
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
		CREATE TABLE test_geo_linestring (
			Col1 LineString
			, Col2 Array(LineString)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_linestring")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_linestring")
		require.NoError(t, err)
		var (
			col1Data = orb.LineString{
				orb.Point{1, 2},
				orb.Point{3, 4},
				orb.Point{5, 6},
			}
			col2Data = []orb.LineString{
				{
					orb.Point{1, 2},
					orb.Point{3, 4},
				},
				{
					orb.Point{5, 6},
					orb.Point{7, 8},
				},
			}
		)
		require.NoError(t, batch.Append(col1Data, col2Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var (
			col1 orb.LineString
			col2 []orb.LineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_linestring").Scan(&col1, &col2))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
	})
}

func TestGeoLineStringFlush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "Flush")
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
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
		CREATE TABLE test_geo_linestring_flush (
			  Col1 LineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_geo_linestring_flush")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_linestring_flush")
		require.NoError(t, err)
		vals := [1000]orb.LineString{}
		for i := 0; i < 1000; i++ {
			vals[i] = orb.LineString{
				orb.Point{float64(i), float64(i + 1)},
				orb.Point{float64(i + 2), float64(i + 3)},
			}
			require.NoError(t, batch.Append(vals[i]))
			require.Equal(t, 1, batch.Rows())
			require.NoError(t, batch.Flush())
		}
		require.Equal(t, 0, batch.Rows())
		require.NoError(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT * FROM test_geo_linestring_flush")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var col1 orb.LineString
			require.NoError(t, rows.Scan(&col1))
			require.Equal(t, vals[i], col1)
			i += 1
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000, i)
	})
}

type testGeoLineStringSerializer struct {
	val orb.LineString
}

func (c testGeoLineStringSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testGeoLineStringSerializer) Scan(src any) error {
	if t, ok := src.(orb.LineString); ok {
		*c = testGeoLineStringSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testGeoLineStringSerializer", src)
}

func TestGeoLineStringValuer(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
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
		CREATE TABLE test_geo_linestring_valuer (
			  Col1 LineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_geo_linestring_valuer")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_linestring_valuer")
		require.NoError(t, err)
		vals := [1000]orb.LineString{}
		for i := 0; i < 1000; i++ {
			vals[i] = orb.LineString{
				orb.Point{float64(i), float64(i + 1)},
				orb.Point{float64(i + 2), float64(i + 3)},
			}
			require.NoError(t, batch.Append(testGeoLineStringSerializer{val: vals[i]}))
		}
		require.NoError(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT * FROM test_geo_linestring_valuer")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var col1 orb.LineString
			require.NoError(t, rows.Scan(&col1))
			require.Equal(t, vals[i], col1)
			i += 1
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000, i)
	})
}

func TestGeoLineStringEmpty(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
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
		CREATE TABLE test_geo_linestring_empty (
			Col1 LineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_linestring_empty")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_linestring_empty")
		require.NoError(t, err)
		var (
			col1Data = orb.LineString{}
		)
		require.NoError(t, batch.Append(col1Data))
		require.NoError(t, batch.Send())
		var (
			col1 orb.LineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_linestring_empty").Scan(&col1))
		assert.Equal(t, col1Data, col1)
	})
}

func TestGeoLineStringSinglePoint(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
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
		CREATE TABLE test_geo_linestring_single_point (
			Col1 LineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_linestring_single_point")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_linestring_single_point")
		require.NoError(t, err)
		var (
			col1Data = orb.LineString{orb.Point{1, 2}}
		)
		require.NoError(t, batch.Append(col1Data))
		require.NoError(t, batch.Send())
		var (
			col1 orb.LineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_linestring_single_point").Scan(&col1))
		assert.Equal(t, col1Data, col1)
	})
}
