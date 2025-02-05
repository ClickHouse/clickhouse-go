package benchmark

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"os"
	"testing"
	"time"
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

	jsonRow := buildTestJSONPaths()
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

var jsonTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

type Address struct {
	Street  string `chType:"String"`
	City    string `chType:"String"`
	Country string `chType:"String"`
}

type TestStruct struct {
	Name   string
	Age    int64
	Active bool
	Score  float64

	Tags    []string
	Numbers []int64

	Address Address

	KeysNumbers map[string]int64
	Metadata    map[string]interface{}

	Timestamp time.Time `chType:"DateTime64(3)"`

	DynamicString chcol.Dynamic
	DynamicInt    chcol.Dynamic
	DynamicMap    chcol.Dynamic
}

func buildTestJSONPaths() *chcol.JSON {
	jsonRow := chcol.NewJSON()
	jsonRow.SetValueAtPath("Name", "JSON")
	jsonRow.SetValueAtPath("Age", int64(42))
	jsonRow.SetValueAtPath("Active", true)
	jsonRow.SetValueAtPath("Score", 3.14)
	jsonRow.SetValueAtPath("Tags", []string{"a", "b"})
	jsonRow.SetValueAtPath("Numbers", []int64{20, 40})
	jsonRow.SetValueAtPath("Address.Street", "Street")
	jsonRow.SetValueAtPath("Address.City", "City")
	jsonRow.SetValueAtPath("Address.Country", "Country")
	jsonRow.SetValueAtPath("KeysNumbers", map[string]int64{"FieldA": 42, "FieldB": 32})
	jsonRow.SetValueAtPath("Metadata.FieldA", "a")
	jsonRow.SetValueAtPath("Metadata.FieldB", "b")
	jsonRow.SetValueAtPath("Metadata.FieldC.FieldD", "d")
	jsonRow.SetValueAtPath("Timestamp", jsonTestDate)
	jsonRow.SetValueAtPath("DynamicString", clickhouse.NewDynamic("str"))
	jsonRow.SetValueAtPath("DynamicInt", clickhouse.NewDynamic(int64(48)))
	jsonRow.SetValueAtPath("DynamicMap", clickhouse.NewDynamic(map[string]string{"a": "a", "b": "b"}))

	return jsonRow
}

func buildTestJSONStruct() TestStruct {
	return TestStruct{
		Name:    "JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: Address{
			Street:  "Street",
			City:    "City",
			Country: "Country",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": "d",
			},
		},
		Timestamp:     jsonTestDate,
		DynamicString: chcol.NewDynamic("str").WithType("String"),
		DynamicInt:    chcol.NewDynamic(int64(48)).WithType("Int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
	}
}

// BenchmarkJSONInsert tests the performance for appending to a JSON column batch
func BenchmarkJSONInsert(b *testing.B) {
	b.Run("paths", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		jsonRow := buildTestJSONPaths()

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

		inputRow := TestStruct{
			Name:    "JSON",
			Age:     42,
			Active:  true,
			Score:   3.14,
			Tags:    []string{"a", "b"},
			Numbers: []int64{20, 40},
			Address: Address{
				Street:  "Street",
				City:    "City",
				Country: "Country",
			},
			KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
			Metadata: map[string]interface{}{
				"FieldA": "a",
				"FieldB": "b",
				"FieldC": map[string]interface{}{
					"FieldD": "d",
				},
			},
			Timestamp:     jsonTestDate,
			DynamicString: chcol.NewDynamic("str").WithType("String"),
			DynamicInt:    chcol.NewDynamic(int64(48)).WithType("Int64"),
			DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batch.Append(inputRow); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	b.Run("marshal_strings", func(b *testing.B) {
		ctx := context.Background()
		conn, batch := prepareJSONInsertTest(ctx, b)
		defer conn.Close()

		inputRow := buildTestJSONStruct()

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

		inputRow := buildTestJSONStruct()

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

			var row TestStruct
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
	b.Run("paths_direct", func(b *testing.B) {
		pathsRow := buildTestJSONPaths()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := pathsRow.MarshalJSON()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("paths", func(b *testing.B) {
		pathsRow := buildTestJSONPaths()

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
		structRow := buildTestJSONStruct()

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
