package column

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

// encodeDynamicBinary is ClickHouse ISerialization::serializeBinary for Dynamic:
// <binary type index><value>. Used to build JSON shared-data payloads the way
// the server writes them into Array(Tuple(String, String)).
func encodeDynamicInt64(v int64) string {
	b := make([]byte, 9)
	b[0] = 0x0A // BinaryTypeIndex::Int64
	binary.LittleEndian.PutUint64(b[1:], uint64(v))
	return string(b)
}

func encodeDynamicFloat64(v float64) string {
	b := make([]byte, 9)
	b[0] = 0x0E // BinaryTypeIndex::Float64
	binary.LittleEndian.PutUint64(b[1:], math.Float64bits(v))
	return string(b)
}

func encodeDynamicString(s string) string {
	var buf proto.Buffer
	buf.PutUInt8(0x15) // BinaryTypeIndex::String
	buf.PutString(s)
	return string(buf.Buf)
}

func encodeDynamicBool(v bool) string {
	b := []byte{0x2D, 0} // BinaryTypeIndex::Bool
	if v {
		b[1] = 1
	}
	return string(b)
}

func encodeDynamicEmptyArrayInt64() string {
	// Array + Int64 + varuint count 0
	return string([]byte{0x1E, 0x0A, 0x00})
}

func encodeSharedData(t *testing.T, rows [][][2]string) []byte {
	t.Helper()
	col, err := Type("Array(Tuple(String, String))").Column("", nil)
	require.NoError(t, err)
	for _, row := range rows {
		pairs := make([][]any, len(row))
		for i, p := range row {
			pairs[i] = []any{p[0], p[1]}
		}
		require.NoError(t, col.AppendRow(pairs))
	}
	var buf proto.Buffer
	col.Encode(&buf)
	return buf.Buf
}

// TestJSONDeprecatedSharedDataConsumedAndMerged is the #1854 repro at the
// native-protocol layer: ClickHouse JSON v0 (deprecated object) stores overflow
// paths in SharedData as Array(Tuple(String, String)). The decoder used to
// skip only the UInt64 offsets and leave the Tuple payload in the stream,
// which desyncs every later column (makeslice panics, "unexpected value N
// for boolean", "unexpected packet N").
func TestJSONDeprecatedSharedDataConsumedAndMerged(t *testing.T) {
	shared := encodeSharedData(t, [][][2]string{
		{
			{"k08", encodeDynamicInt64(8)},
			{"satellites", encodeDynamicInt64(11)},
			{"region", encodeDynamicString("us-east")},
			{"ok", encodeDynamicBool(true)},
			{"ratio", encodeDynamicFloat64(12.5)},
			{"latency_ms", encodeDynamicEmptyArrayInt64()},
		},
	})

	const sentinel byte = 0xAB
	payload := append(append([]byte{}, shared...), sentinel)

	col := newTestJSONColumn(t)
	col.serializationVersion = JSONDeprecatedObjectSerializationVersion

	reader := proto.NewReader(bytes.NewReader(payload))
	require.NoError(t, col.Decode(reader, 1))

	got, err := reader.ReadByte()
	require.NoError(t, err, "shared-data payload must be fully consumed")
	require.Equal(t, sentinel, got, "bytes after the JSON column must stay aligned for the next column")

	obj, ok := col.Row(0, false).(*chcol.JSON)
	require.True(t, ok)

	k08, ok := chcol.ExtractJSONPathAs[int64](obj, "k08")
	require.True(t, ok, "overflow path k08 must be merged from shared data")
	require.Equal(t, int64(8), k08)

	sat, ok := chcol.ExtractJSONPathAs[int64](obj, "satellites")
	require.True(t, ok)
	require.Equal(t, int64(11), sat)

	region, ok := chcol.ExtractJSONPathAs[string](obj, "region")
	require.True(t, ok)
	require.Equal(t, "us-east", region)

	flag, ok := chcol.ExtractJSONPathAs[bool](obj, "ok")
	require.True(t, ok)
	require.Equal(t, true, flag)

	ratio, ok := chcol.ExtractJSONPathAs[float64](obj, "ratio")
	require.True(t, ok)
	require.Equal(t, 12.5, ratio)

	latency, ok := chcol.ExtractJSONPathAs[[]any](obj, "latency_ms")
	require.True(t, ok)
	require.Empty(t, latency)
}

func TestJSONDeprecatedSharedDataEmptyRoundtrip(t *testing.T) {
	src := newTestJSONColumn(t)
	require.NoError(t, src.AppendRow(map[string]any{"kept": int64(7)}))

	var buf proto.Buffer
	require.NoError(t, src.WriteStatePrefix(&buf))
	src.Encode(&buf)

	dst := newTestJSONColumn(t)
	reader := proto.NewReader(bytes.NewReader(buf.Buf))
	require.NoError(t, dst.ReadStatePrefix(reader))
	require.NoError(t, dst.Decode(reader, 1))

	obj, ok := dst.Row(0, false).(*chcol.JSON)
	require.True(t, ok)
	kept, ok := chcol.ExtractJSONPathAs[int64](obj, "kept")
	require.True(t, ok)
	require.Equal(t, int64(7), kept)
}

func TestJSONDeprecatedSharedDataSecondRowOnly(t *testing.T) {
	shared := encodeSharedData(t, [][][2]string{
		{},
		{{"overflow", encodeDynamicInt64(42)}},
	})

	const sentinel byte = 0xCD
	payload := append(append([]byte{}, shared...), sentinel)

	col := newTestJSONColumn(t)
	col.serializationVersion = JSONDeprecatedObjectSerializationVersion

	reader := proto.NewReader(bytes.NewReader(payload))
	require.NoError(t, col.Decode(reader, 2))

	got, err := reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, sentinel, got)

	row0, ok := col.Row(0, false).(*chcol.JSON)
	require.True(t, ok)
	_, present := row0.ValueAtPath("overflow")
	require.False(t, present, "row 0 has empty shared data")

	row1, ok := col.Row(1, false).(*chcol.JSON)
	require.True(t, ok)
	v, ok := chcol.ExtractJSONPathAs[int64](row1, "overflow")
	require.True(t, ok)
	require.Equal(t, int64(42), v)
}
