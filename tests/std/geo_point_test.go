package std

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestStdGeoPoint(t *testing.T) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}))
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE TEMPORARY TABLE test_geo_point (
			Col1 Point
			, Col2 Array(Point)
		)
		`
		if _, err := conn.ExecContext(ctx, ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_geo_point"); assert.NoError(t, err) {
				if _, err := batch.Exec(
					orb.Point{11, 22},
					[]orb.Point{
						{1, 2},
						{3, 4},
					},
				); assert.NoError(t, err) {
					if assert.NoError(t, scope.Commit()) {
						var (
							col1 orb.Point
							col2 []orb.Point
						)
						if err := conn.QueryRow("SELECT * FROM test_geo_point").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, orb.Point{11, 22}, col1)
							assert.Equal(t, []orb.Point{
								{1, 2},
								{3, 4},
							}, col2)
						}
					}
				}
			}
		}
	}
}
