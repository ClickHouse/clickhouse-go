package std

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestStdGeoRing(t *testing.T) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}))
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TEMPORARY TABLE test_geo_ring (
			Col1 Ring
			, Col2 Array(Ring)
		)
		`
		if _, err := conn.ExecContext(ctx, ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_geo_ring"); assert.NoError(t, err) {
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
				if _, err := batch.Exec(col1Data, col2Data); assert.NoError(t, err) {
					if assert.NoError(t, scope.Commit()) {
						var (
							col1 orb.Ring
							col2 []orb.Ring
						)
						if err := conn.QueryRow("SELECT * FROM test_geo_ring").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
						}
					}
				}
			}
		}
	}
}
