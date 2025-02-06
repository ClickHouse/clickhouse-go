package benchmark

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"os"
	"testing"
)

const testSet string = "json_bench"

func TestMain(m *testing.M) {
	os.Exit(clickhouse_tests.Runtime(m, testSet))
}

func GetNativeConnection(settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	return clickhouse_tests.GetConnection(testSet, settings, tlsConfig, compression)
}

func prepareJSONTest(ctx context.Context, b *testing.B) driver.Conn {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":           60,
		"allow_experimental_json_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	if err != nil {
		b.Fatal(err)
	}

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 9, 0) {
		b.Skip("unsupported clickhouse version for JSON type")
	}

	err = conn.Exec(ctx, "DROP TABLE IF EXISTS go_json_bench")
	if err != nil {
		b.Fatal(err)
	}

	return conn
}

func prepareJSONInsertTest(ctx context.Context, b *testing.B) (driver.Conn, driver.Batch) {
	conn := prepareJSONTest(ctx, b)

	err := conn.Exec(ctx, `
		CREATE TABLE go_json_bench (obj JSON) ENGINE=Null
		`)
	if err != nil {
		b.Fatal(err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_bench (obj)")
	if err != nil {
		b.Fatal(err)
	}

	return conn, batch
}

func prepareJSONReadTest(ctx context.Context, b *testing.B) (driver.Conn, driver.Rows) {
	conn := prepareJSONTest(ctx, b)

	err := conn.Exec(ctx, `
		CREATE TABLE go_json_bench (obj JSON) ENGINE=Memory
		`)
	if err != nil {
		b.Fatal(err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO go_json_bench (obj)")
	if err != nil {
		b.Fatal(err)
	}

	jsonRow := clickhouse_tests.BuildFastTestJSONStruct()
	for i := 0; i < b.N; i++ {
		if err := batch.Append(jsonRow); err != nil {
			b.Fatal(err)
		}
	}

	if err := batch.Send(); err != nil {
		b.Fatal(err)
	}

	rows, err := conn.Query(ctx, "SELECT obj FROM go_json_bench")
	if err != nil {
		b.Fatal(err)
	}

	return conn, rows
}

// BenchmarkJSONInsert tests the performance for appending to a JSON column batch
func BenchmarkJSONInsert(b *testing.B) {
	b.Run("paths", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		jsonRow := clickhouse_tests.BuildTestJSONPaths()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batch.Append(jsonRow); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("structs", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		inputRow := clickhouse_tests.BuildTestJSONStruct()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batch.Append(inputRow); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("fast_structs", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		inputRow := clickhouse_tests.BuildFastTestJSONStruct()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batch.Append(&inputRow); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("marshal_strings", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		inputRow := clickhouse_tests.BuildTestJSONStruct()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inputRowStr, err := json.Marshal(inputRow)
			if err != nil {
				b.Fatal(err)
			}

			if err := batch.Append(inputRowStr); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("strings", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		inputRow := clickhouse_tests.BuildTestJSONStruct()

		inputRowStr, err := json.Marshal(inputRow)
		if err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batch.Append(inputRowStr); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

// BenchmarkJSONInsert tests the performance for scanning rows from a JSON column
func BenchmarkJSONRead(b *testing.B) {
	b.Run("paths", func(b *testing.B) {
		ctx := context.Background()
		conn, rows := prepareJSONReadTest(ctx, b)
		defer conn.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows.Next()

			var row clickhouse.JSON
			err := rows.Scan(&row)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("structs", func(b *testing.B) {
		ctx := context.Background()
		conn, rows := prepareJSONReadTest(ctx, b)
		defer conn.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows.Next()

			var row clickhouse_tests.TestStruct
			err := rows.Scan(&row)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("fast_structs", func(b *testing.B) {
		ctx := context.Background()
		conn, rows := prepareJSONReadTest(ctx, b)
		defer conn.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows.Next()

			var row clickhouse_tests.FastTestStruct
			err := rows.Scan(&row)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("marshal_strings", func(b *testing.B) {
		b.Skip("cannot receive JSON strings")
	})

	b.Run("strings", func(b *testing.B) {
		b.Skip("cannot receive JSON strings")
	})
}

// BenchmarkJSONMarshal compares the different ways to turn JSON data back into a string
func BenchmarkJSONMarshal(b *testing.B) {
	b.Run("paths", func(b *testing.B) {
		pathsRow := clickhouse_tests.BuildTestJSONPaths()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := json.Marshal(pathsRow)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("structs", func(b *testing.B) {
		structRow := clickhouse_tests.BuildTestJSONStruct()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := json.Marshal(structRow)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
