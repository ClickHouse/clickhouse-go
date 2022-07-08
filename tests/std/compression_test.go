package std

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCompressionHttpStd(t *testing.T) {
	interfaces := map[clickhouse.InterfaceType]int{clickhouse.HttpInterface: 8123, clickhouse.NativeInterface: 9000}
	for interfaceType, port := range interfaces {
		conn := clickhouse.OpenDB(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("127.0.0.1:%d", port)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 5 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Interface: interfaceType,
		})
		conn.Exec("DROP TABLE test_array")
		const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec("DROP TABLE test_array")
		}()
		_, err := conn.Exec(ddl)
		require.NoError(t, err)
		scope, err := conn.Begin()
		require.NoError(t, err)
		batch, err := scope.Prepare("INSERT INTO test_array")
		require.NoError(t, err)
		var (
			col1Data = []string{"A", "b", "c"}
		)
		for i := 0; i < 100; i++ {
			_, err := batch.Exec(col1Data)
			require.NoError(t, err)
		}
		require.NoError(t, scope.Commit())
		rows, err := conn.Query("SELECT * FROM test_array")
		require.NoError(t, err)
		for rows.Next() {
			var (
				col1 interface{}
			)
			require.NoError(t, rows.Scan(&col1))
			assert.Equal(t, col1Data, col1)
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	}
}

//test compression over std with dsn and compress
