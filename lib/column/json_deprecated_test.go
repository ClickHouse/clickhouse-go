package column

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

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

func TestJSONDeprecatedSharedDataError(t *testing.T) {
	for _, rows := range [][][][2]string{
		{{{"overflow", "\x0a\x0d\x00\x00\x00\x00\x00\x00\x00"}, {"other", "unreadable"}}},
		{{}, {{"overflow", "unreadable"}}},
	} {
		payload := encodeSharedData(t, rows)
		col := newTestJSONColumn(t)
		col.serializationVersion = JSONDeprecatedObjectSerializationVersion
		reader := proto.NewReader(bytes.NewReader(payload))
		err := col.Decode(reader, len(rows))
		require.ErrorContains(t, err, "cannot decode JSON shared data")
		require.ErrorContains(t, err, "25.6")
		require.ErrorContains(t, err, "output_format_native_use_flattened_dynamic_and_json_serialization=1")
		require.ErrorContains(t, err, "changeable_in_readonly")
	}
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

func TestJSONDeprecatedSharedDataMalformed(t *testing.T) {
	var offsets [16]byte
	binary.LittleEndian.PutUint64(offsets[:], 2)
	binary.LittleEndian.PutUint64(offsets[8:], 1)
	col := newTestJSONColumn(t)
	require.ErrorContains(t, col.decodeSharedData_v1(proto.NewReader(bytes.NewReader(offsets[:])), 2), "invalid shared data offsets")
	for _, payload := range [][]byte{nil, offsets[:7]} {
		require.ErrorContains(t, col.decodeSharedData_v1(proto.NewReader(bytes.NewReader(payload)), 1), "failed to read shared data offsets")
	}
}

func TestDynamicDeprecatedSharedVariantError(t *testing.T) {
	for _, raw := range []string{"", "unreadable", "\x0a\x0d\x00\x00\x00\x00\x00\x00\x00"} {
		var buf proto.Buffer
		buf.PutByte(0)
		buf.PutString(raw)
		col := &Dynamic{columns: []Interface{&SharedVariant{}}, columnIndexByType: map[string]int{"SharedVariant": 0}}
		reader := proto.NewReader(bytes.NewReader(buf.Buf))
		require.ErrorContains(t, col.decodeData_v1(reader, 1), "cannot decode Dynamic SharedVariant")
	}
}
