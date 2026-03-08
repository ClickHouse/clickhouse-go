package std

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdArray(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			const ddl = `
		CREATE TABLE std_test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
		) Engine MergeTree() ORDER BY tuple()
		`
			conn.Exec("DROP TABLE std_test_array")
			defer func() {
				conn.Exec("DROP TABLE std_test_array")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_array")
			require.NoError(t, err)
			var (
				timestamp = time.Now().Truncate(time.Second).In(time.UTC)
				col1Data  = []string{"A", "b", "c"}
				col2Data  = [][]uint32{
					{1, 2},
					{3, 87},
					{33, 3, 847},
				}
				col3Data = [][][]time.Time{
					{
						[]time.Time{
							timestamp,
							timestamp,
							timestamp,
							timestamp,
						},
					},
					{
						[]time.Time{
							timestamp,
							timestamp,
							timestamp,
						},
						[]time.Time{
							timestamp,
							timestamp,
						},
					},
				}
			)
			for i := 0; i < 10; i++ {
				_, err := batch.Exec(col1Data, col2Data, col3Data)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM std_test_array")
			require.NoError(t, err)
			for rows.Next() {
				var (
					col1 any
					col2 [][]uint32
					col3 [][][]time.Time
				)
				require.NoError(t, rows.Scan(&col1, &col2, &col3))
				assert.Equal(t, col1Data, col1)
				assert.Equal(t, col2Data, col2)
				assert.Equal(t, col3Data, col3)
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}
