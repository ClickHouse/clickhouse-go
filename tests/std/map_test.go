package std

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"

	"github.com/stretchr/testify/assert"
)

func TestStdMap(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, url.Values{})
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 21, 9, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
		CREATE TABLE std_test_map (
			  Col1 Map(String, UInt64)
			, Col2 Map(String, UInt64)
			, Col3 Map(String, UInt64)
			, Col4 Array(Map(String, String))
			, Col5 Map(LowCardinality(String), LowCardinality(String))
		) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE std_test_map")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_map")
			require.NoError(t, err)
			var (
				col1Data = map[string]uint64{
					"key_col_1_1": 1,
					"key_col_1_2": 2,
				}
				col2Data = map[string]uint64{
					"key_col_2_1": 10,
					"key_col_2_2": 20,
				}
				col3Data = map[string]uint64{}
				col4Data = []map[string]string{
					{"A": "B"},
					{"C": "D"},
				}
				col5Data = map[string]string{
					"key_col_5_1": "100",
					"key_col_5_2": "200",
				}
			)
			_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 any
				col2 map[string]uint64
				col3 map[string]uint64
				col4 []map[string]string
				col5 map[string]string
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM std_test_map").Scan(&col1, &col2, &col3, &col4, &col5))
			assert.Equal(t, col1Data, col1)
			assert.Equal(t, col2Data, col2)
			assert.Equal(t, col3Data, col3)
			assert.Equal(t, col4Data, col4)
			assert.Equal(t, col5Data, col5)
		})
	}
}

func TestStdMapParameterizedKey(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, url.Values{})
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			if !CheckMinServerVersion(conn, 24, 5, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}

			const ddl = `
		CREATE TABLE std_test_map_parameterized_key (
			  Col1 Map(DateTime64(3, 'UTC'), String)
		) Engine MergeTree() ORDER BY tuple()
		`
			const table = "std_test_map_parameterized_key"
			t.Cleanup(func() {
				if _, err := conn.Exec("DROP TABLE IF EXISTS " + table); err != nil {
					t.Logf("failed to drop %s: %v", table, err)
				}
			})

			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO " + table)
			require.NoError(t, err)
			input := map[time.Time]string{
				time.Date(2020, 1, 2, 3, 4, 5, 123000000, time.UTC): "value",
			}
			_, err = batch.Exec(input)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())

			var output map[time.Time]string
			require.NoError(t, conn.QueryRow("SELECT * FROM "+table).Scan(&output))
			assert.Equal(t, input, output)
		})
	}
}

func TestStdInsertNilMap(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, url.Values{})
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 21, 9, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
		CREATE TABLE std_test_map_nil (
			  Col1 Map(String, UInt64)
		) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE std_test_map_nil")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_map_nil")
			require.NoError(t, err)
			_, err = batch.Exec(nil)
			// We started supporting nil to Map
			// see https://github.com/ClickHouse/clickhouse-go/pull/1667
			require.NoError(t, err)
		})
	}
}
