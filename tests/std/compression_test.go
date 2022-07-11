package std

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCompressionStd(t *testing.T) {
	protocols := map[clickhouse.Protocol]int{clickhouse.HTTP: 8123, clickhouse.Native: 9000}
	for protocol, port := range protocols {
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
			Protocol: protocol,
		})
		conn.Exec("DROP TABLE IF EXISTS test_array_compress")
		const ddl = `
		CREATE TABLE test_array_compress (
			  Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec("DROP TABLE test_array_compress")
		}()
		_, err := conn.Exec(ddl)
		require.NoError(t, err)
		scope, err := conn.Begin()
		require.NoError(t, err)
		batch, err := scope.Prepare("INSERT INTO test_array_compress")
		require.NoError(t, err)
		var (
			col1Data = []string{"A", "b", "c"}
		)
		for i := 0; i < 100; i++ {
			_, err := batch.Exec(col1Data)
			require.NoError(t, err)
		}
		require.NoError(t, scope.Commit())
		rows, err := conn.Query("SELECT * FROM test_array_compress")
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

func TestZSTDCompressionStd(t *testing.T) {
	protocols := map[clickhouse.Protocol]int{clickhouse.HTTP: 8123, clickhouse.Native: 9000}
	for protocol, port := range protocols {
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
				Method: clickhouse.CompressionZSTD,
			},
			Protocol: protocol,
		})
		conn.Exec("DROP TABLE IF EXISTS test_array_compress")
		const ddl = `
		CREATE TABLE test_array_compress (
			  Col1 Array(String)
		) Engine Memory
		`
		defer func() {
			conn.Exec("DROP TABLE test_array_compress")
		}()
		_, err := conn.Exec(ddl)
		require.NoError(t, err)
		scope, err := conn.Begin()
		require.NoError(t, err)
		batch, err := scope.Prepare("INSERT INTO test_array_compress")
		require.NoError(t, err)
		var (
			col1Data = []string{"A", "b", "c"}
		)
		for i := 0; i < 100; i++ {
			_, err := batch.Exec(col1Data)
			require.NoError(t, err)
		}
		require.NoError(t, scope.Commit())
		rows, err := conn.Query("SELECT * FROM test_array_compress")
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

func TestCompressionStdDSN(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000?compress=true", "Http": "http://127.0.0.1:8123?compress=true"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := sql.Open("clickhouse", dsn)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS  test_array_compress")
			const ddl = `
				CREATE TABLE test_array_compress (
					  Col1 Array(String)
				) Engine Memory
				`
			defer func() {
				conn.Exec("DROP TABLE test_array_compress")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_array_compress")
			require.NoError(t, err)
			var (
				col1Data = []string{"A", "b", "c"}
			)
			for i := 0; i < 100; i++ {
				_, err := batch.Exec(col1Data)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM test_array_compress")
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
		})
	}

}
