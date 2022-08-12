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
	type compressionTest struct {
		port               int
		compressionMethods []clickhouse.CompressionMethod
	}

	protocols := map[clickhouse.Protocol]compressionTest{clickhouse.HTTP: {
		port:               8123,
		compressionMethods: []clickhouse.CompressionMethod{clickhouse.CompressionLZ4, clickhouse.CompressionZSTD, clickhouse.CompressionGZIP, clickhouse.CompressionDeflate, clickhouse.CompressionBrotli},
	}, clickhouse.Native: {
		port:               9000,
		compressionMethods: []clickhouse.CompressionMethod{clickhouse.CompressionLZ4, clickhouse.CompressionZSTD},
	}}
	for protocol, compressionTest := range protocols {
		for _, method := range compressionTest.compressionMethods {
			t.Run(fmt.Sprintf("%s with %s", protocol, method), func(t *testing.T) {
				conn := clickhouse.OpenDB(&clickhouse.Options{
					Addr: []string{fmt.Sprintf("127.0.0.1:%d", compressionTest.port)},
					Auth: clickhouse.Auth{
						Database: "default",
						Username: "default",
						Password: "",
					},
					Settings: clickhouse.Settings{
						"max_execution_time":      60,
						"enable_http_compression": 1, // needed for http compression e.g. gzip
					},
					DialTimeout: 5 * time.Second,
					Compression: &clickhouse.Compression{
						Method: method,
						Level:  3,
					},
					Protocol: protocol,
				})
				conn.Exec("DROP TABLE IF EXISTS test_array_compress")
				const ddl = `
					CREATE TABLE test_array_compress (
						  Col1 Array(Int32),
					      Col2 Int32         
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
				for i := int32(0); i < 100; i++ {
					_, err := batch.Exec([]int32{i, i + 1, i + 2}, i)
					require.NoError(t, err)
				}
				require.NoError(t, scope.Commit())
				rows, err := conn.Query("SELECT * FROM test_array_compress ORDER BY Col2 ASC")
				require.NoError(t, err)
				i := int32(0)
				for rows.Next() {
					var (
						col1 interface{}
						col2 int32
					)
					require.NoError(t, rows.Scan(&col1, &col2))
					assert.Equal(t, i, col2)
					assert.Equal(t, []int32{i, i + 1, i + 2}, col1)
					i += 1
				}
				require.NoError(t, rows.Close())
				require.NoError(t, rows.Err())
				scope, err = conn.Begin()
				require.NoError(t, err)
				batch, err = scope.Prepare("INSERT INTO test_array_compress")
				require.NoError(t, err)
				for i := int32(100); i < 200; i++ {
					_, err := batch.Exec([]int32{i, i + 1, i + 2}, i)
					require.NoError(t, err)
				}
				require.NoError(t, scope.Commit())
				require.NoError(t, err)
				i = 0
				for rows.Next() {
					var (
						col1 interface{}
						col2 int32
					)
					require.NoError(t, rows.Scan(&col1, &col2))
					assert.Equal(t, i, col2)
					assert.Equal(t, []int32{i, i + 1, i + 2}, col1)
					i += 1
				}
			})
		}
	}
}

func TestCompressionStdDSN(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000?compress=true", "Http": "http://127.0.0.1:8123?compress=true"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := sql.Open("clickhouse", dsn)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS test_array_compress")
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

func TestCompressionStdDSNWithLevel(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000?compress=lz4", "Http": "http://127.0.0.1:8123?compress=gzip&compress_level=9"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := sql.Open("clickhouse", dsn)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS test_array_compress")
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

func TestCompressionStdDSNInvalid(t *testing.T) {
	// these should all fail
	config := map[string][]string{"Native": {"clickhouse://127.0.0.1:9000?compress=gzip"},
		"Http": {"http://127.0.0.1:8123?compress=gzip&compress_level=10",
			"http://127.0.0.1:8123?compress=gzip&compress_level=-3"}}
	for name, dsns := range config {
		for _, dsn := range dsns {
			t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
				conn, err := sql.Open("clickhouse", dsn)
				const ddl = `
				CREATE TABLE test_array_compress (
					  Col1 Array(String)
				) Engine Memory
				`
				_, err = conn.Exec(ddl)
				require.Error(t, err)
			})
		}
	}
}
