package column

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

// newBenchJSONColumn builds a fresh JSON column for each benchmark. The
// column is reset inside the hot loop so the typed/dynamic sub-columns and
// the jsonStrings String column do not grow unboundedly during a long -benchtime.
// The server context is pinned to 25.6 so the flat dynamic JSON code path
// (the current production layout) is exercised.
func newBenchJSONColumn(b *testing.B) *JSON {
	b.Helper()
	sc := &ServerContext{VersionMajor: 25, VersionMinor: 6}
	col, err := (&JSON{name: "bench"}).parse("JSON", sc)
	if err != nil {
		b.Fatalf("parse JSON column: %v", err)
	}
	return col
}

// resetEveryN clears column state every N iterations to keep the benchmark
// measuring the append path rather than the cost of accumulating rows.
const resetEveryN = 1024

func benchLoop(b *testing.B, col *JSON, v any) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.AppendRow(v); err != nil {
			b.Fatalf("AppendRow: %v", err)
		}
		if (i+1)%resetEveryN == 0 {
			col.Reset()
		}
	}
}

func BenchmarkJSONAppendRow_String(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := `{"id":1,"name":"Book","tags":["a","b"]}`
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_StringPointer(b *testing.B) {
	// Exercises the *string path — the exact Issue 1 reproducer.
	col := newBenchJSONColumn(b)
	s := `{"id":1,"name":"Book","tags":["a","b"]}`
	v := &s
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_Bytes(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := []byte(`{"id":1,"name":"Book","tags":["a","b"]}`)
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_BytesPointer(b *testing.B) {
	col := newBenchJSONColumn(b)
	bs := []byte(`{"id":1,"name":"Book","tags":["a","b"]}`)
	v := &bs
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_RawMessage(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := json.RawMessage(`{"id":1,"name":"Book","tags":["a","b"]}`)
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_NullString(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := sql.NullString{Valid: true, String: `{"id":1,"name":"Book"}`}
	benchLoop(b, col, v)
}

// benchStruct uses only string fields so the dynamic subcolumn path is
// uniform across iterations. Mixed types (int+string) require a
// real ClickHouse server to resolve type variants.
type benchStruct struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func BenchmarkJSONAppendRow_Struct(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := benchStruct{ID: "1", Name: "Book"}
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_StructPointer(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := &benchStruct{ID: "1", Name: "Book"}
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_Map(b *testing.B) {
	col := newBenchJSONColumn(b)
	v := map[string]any{"id": "1", "name": "Book"}
	benchLoop(b, col, v)
}

func BenchmarkJSONAppendRow_ChcolJSON(b *testing.B) {
	col := newBenchJSONColumn(b)
	obj := chcol.NewJSON()
	obj.SetValueAtPath("id", "1")
	obj.SetValueAtPath("name", "Book")
	benchLoop(b, col, obj)
}

func BenchmarkJSONAppendRow_Nil(b *testing.B) {
	col := newBenchJSONColumn(b)
	benchLoop(b, col, nil)
}

// BenchmarkJSONAppendRow_AlternatingNilStruct mirrors a realistic
// Nullable(JSON) insert pattern with mixed null and struct rows.
func BenchmarkJSONAppendRow_AlternatingNilStruct(b *testing.B) {
	col := newBenchJSONColumn(b)
	s := benchStruct{ID: "1", Name: "Book"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v any
		if i%2 == 0 {
			v = nil
		} else {
			v = s
		}
		if err := col.AppendRow(v); err != nil {
			b.Fatalf("AppendRow: %v", err)
		}
		if (i+1)%resetEveryN == 0 {
			col.Reset()
		}
	}
}

// The benchmarks below exercise the plural Append (columnar bulk insert)
// path — same code that W&B would hit when building a batch via
// batch.Column(i).Append(slice).

// Batch size per Append call in the bulk benchmarks. Each iteration
// reports the per-slice cost, not per-row.
const benchBatch = 64

func BenchmarkJSONAppend_StringSlice(b *testing.B) {
	col := newBenchJSONColumn(b)
	slice := make([]string, benchBatch)
	for i := range slice {
		slice[i] = `{"id":1,"name":"Book"}`
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(slice); err != nil {
			b.Fatalf("Append: %v", err)
		}
		col.Reset()
	}
}

func BenchmarkJSONAppend_StringPointerSlice(b *testing.B) {
	// Before the fix: silent {} for every element via reflect.Value bug.
	col := newBenchJSONColumn(b)
	slice := make([]*string, benchBatch)
	for i := range slice {
		s := `{"id":1,"name":"Book"}`
		slice[i] = &s
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(slice); err != nil {
			b.Fatalf("Append: %v", err)
		}
		col.Reset()
	}
}

func BenchmarkJSONAppend_RawMessageSlice(b *testing.B) {
	col := newBenchJSONColumn(b)
	slice := make([]json.RawMessage, benchBatch)
	for i := range slice {
		slice[i] = json.RawMessage(`{"id":1,"name":"Book"}`)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(slice); err != nil {
			b.Fatalf("Append: %v", err)
		}
		col.Reset()
	}
}

func BenchmarkJSONAppend_StructSlice(b *testing.B) {
	// Before the fix: silent {} — every struct's exported fields dropped.
	col := newBenchJSONColumn(b)
	slice := make([]benchStruct, benchBatch)
	for i := range slice {
		slice[i] = benchStruct{ID: "1", Name: "Book"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(slice); err != nil {
			b.Fatalf("Append: %v", err)
		}
		col.Reset()
	}
}
