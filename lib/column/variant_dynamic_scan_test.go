package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

func TestVariantDynamicPointerScan(t *testing.T) {
	columns := []struct {
		name string
		typ  Type
		sc   *ServerContext
	}{
		{name: "Variant", typ: "Variant(Int64, String)"},
		{name: "Dynamic v1", typ: "Dynamic", sc: &ServerContext{VersionMajor: 25, VersionMinor: 5}},
		{name: "Dynamic v3", typ: "Dynamic", sc: &ServerContext{VersionMajor: 25, VersionMinor: 6}},
	}
	sequences := []struct {
		name   string
		values []any
		types  []string
	}{
		{name: "value first", values: []any{int64(42), "hello", nil, int64(84)}, types: []string{"Int64", "String", "", "Int64"}},
		{name: "null first", values: []any{nil, int64(42), "", nil}, types: []string{"", "Int64", "String", ""}},
		{name: "all null", values: []any{nil, nil}, types: []string{"", ""}},
	}

	for _, column := range columns {
		t.Run(column.name, func(t *testing.T) {
			for _, sequence := range sequences {
				t.Run(sequence.name, func(t *testing.T) {
					writer, err := column.typ.Column("value", column.sc)
					require.NoError(t, err)
					for i, value := range sequence.values {
						require.NoError(t, writer.AppendRow(chcol.NewVariantWithType(value, sequence.types[i])))
					}
					var buffer proto.Buffer
					require.NoError(t, writer.(CustomSerialization).WriteStatePrefix(&buffer))
					writer.Encode(&buffer)

					reader, err := column.typ.Column("value", column.sc)
					require.NoError(t, err)
					stream := proto.NewReader(bytes.NewReader(buffer.Buf))
					require.NoError(t, reader.(CustomSerialization).ReadStatePrefix(stream))
					require.NoError(t, reader.Decode(stream, len(sequence.values)))

					for _, initialized := range []bool{false, true} {
						name := "nil destination"
						if initialized {
							name = "existing destination"
						}
						t.Run(name, func(t *testing.T) {
							var got *chcol.Variant
							if initialized {
								value := chcol.NewVariantWithType("stale", "String")
								got = &value
							}
							for i, value := range sequence.values {
								previous := got
								require.NotPanics(t, func() { require.NoError(t, reader.ScanRow(&got, i)) })
								require.NotNil(t, got)
								assert.Equal(t, value, got.Any())
								assert.Equal(t, sequence.types[i], got.Type())
								assert.Equal(t, value == nil, got.Nil())
								if previous != nil {
									assert.Same(t, previous, got)
								}
								var direct chcol.Variant
								require.NoError(t, reader.ScanRow(&direct, i))
								assert.Equal(t, direct, *got)
							}
						})
					}
				})
			}
		})
	}
}
