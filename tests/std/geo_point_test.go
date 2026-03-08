package std

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
)

func TestStdGeoPoint(t *testing.T) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_geo_types": 1,
	}))
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 21, 12, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
				CREATE TABLE std_test_geo_point (
					Col1 Point
					, Col2 Array(Point)
				) Engine MergeTree() ORDER BY tuple()
				`
			defer func() {
				conn.Exec("DROP TABLE std_test_geo_point")
			}()
			_, err = conn.ExecContext(ctx, ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_geo_point")
			require.NoError(t, err)
			_, err = batch.Exec(
				orb.Point{11, 22},
				[]orb.Point{
					{1, 2},
					{3, 4},
				},
			)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 orb.Point
				col2 []orb.Point
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM std_test_geo_point").Scan(&col1, &col2))
			assert.Equal(t, orb.Point{11, 22}, col1)
			assert.Equal(t, []orb.Point{
				{1, 2},
				{3, 4},
			}, col2)
		})
	}
}
