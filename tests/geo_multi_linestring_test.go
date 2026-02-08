package tests

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestGeoMultiLineString(t *testing.T) {
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
		CREATE TABLE test_geo_multi_linestring (
			  Col1 MultiLineString
			, Col2 Array(MultiLineString)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_geo_multi_linestring")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_multi_linestring")
		require.NoError(t, err)
		var (
			col1Data = orb.MultiLineString{
				orb.LineString{
					orb.Point{1, 2},
					orb.Point{12, 2},
				},
				orb.LineString{
					orb.Point{11, 2},
					orb.Point{1, 12},
				},
			}
			col2Data = []orb.MultiLineString{
				{
					orb.LineString{
						orb.Point{1, 2},
						orb.Point{1, 22},
					},
					orb.LineString{
						orb.Point{1, 23},
						orb.Point{12, 2},
					},
				},
				{
					orb.LineString{
						orb.Point{21, 2},
						orb.Point{1, 222},
					},
					orb.LineString{
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
			col1 orb.MultiLineString
			col2 []orb.MultiLineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_multi_linestring").Scan(&col1, &col2))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
	})
}

func TestGeoMultiLineStringFlush(t *testing.T) {
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
		CREATE TABLE test_geo_multi_linestring_flush (
			  Col1 MultiLineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_geo_multi_linestring_flush")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_multi_linestring_flush")
		require.NoError(t, err)
		vals := [1000]orb.MultiLineString{}
		for i := 0; i < 1000; i++ {
			vals[i] = orb.MultiLineString{
				orb.LineString{
					orb.Point{rand.Float64(), rand.Float64()},
					orb.Point{rand.Float64(), rand.Float64()},
				},
				orb.LineString{
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
		rows, err := conn.Query(ctx, "SELECT * FROM test_geo_multi_linestring_flush")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var col1 orb.MultiLineString
			require.NoError(t, rows.Scan(&col1))
			require.Equal(t, vals[i], col1)
			i += 1
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000, i)
	})
}

type testMultiLineStringSerializer struct {
	val orb.MultiLineString
}

func (c testMultiLineStringSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testMultiLineStringSerializer) Scan(src any) error {
	if t, ok := src.(orb.MultiLineString); ok {
		*c = testMultiLineStringSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testMultiLineStringSerializer", src)
}

func TestGeoMultiLineStringValuer(t *testing.T) {
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
		CREATE TABLE test_geo_multi_linestring_valuer (
			  Col1 MultiLineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_geo_multi_linestring_valuer")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_multi_linestring_valuer")
		require.NoError(t, err)
		vals := [1000]orb.MultiLineString{}
		for i := 0; i < 1000; i++ {
			vals[i] = orb.MultiLineString{
				orb.LineString{
					orb.Point{rand.Float64(), rand.Float64()},
					orb.Point{rand.Float64(), rand.Float64()},
				},
				orb.LineString{
					orb.Point{rand.Float64(), rand.Float64()},
					orb.Point{rand.Float64(), rand.Float64()},
				},
			}
			require.NoError(t, batch.Append(testMultiLineStringSerializer{val: vals[i]}))
		}
		require.NoError(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT * FROM test_geo_multi_linestring_valuer")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var col1 orb.MultiLineString
			require.NoError(t, rows.Scan(&col1))
			require.Equal(t, vals[i], col1)
			i += 1
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000, i)
	})
}

func TestGeoMultiLineStringEmpty(t *testing.T) {
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
		CREATE TABLE test_geo_multi_linestring_empty (
			Col1 MultiLineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_multi_linestring_empty")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_multi_linestring_empty")
		require.NoError(t, err)
		var (
			col1Data = orb.MultiLineString{}
		)
		require.NoError(t, batch.Append(col1Data))
		require.NoError(t, batch.Send())
		var (
			col1 orb.MultiLineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_multi_linestring_empty").Scan(&col1))
		assert.Equal(t, col1Data, col1)
	})
}

func TestGeoMultiLineStringSinglePoint(t *testing.T) {
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
		CREATE TABLE test_geo_multi_linestring_single_point (
			Col1 MultiLineString
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_geo_multi_linestring_single_point")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_multi_linestring_single_point")
		require.NoError(t, err)
		var (
			col1Data = orb.MultiLineString{
				orb.LineString{orb.Point{1, 2}},
			}
		)
		require.NoError(t, batch.Append(col1Data))
		require.NoError(t, batch.Send())
		var (
			col1 orb.MultiLineString
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_geo_multi_linestring_single_point").Scan(&col1))
		assert.Equal(t, col1Data, col1)
	})
}
