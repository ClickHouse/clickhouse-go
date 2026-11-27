package column

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTupleScanRowReturnsMapKeyError(t *testing.T) {
	col, err := Type("Tuple(name String, value UInt64)").Column("tuple", nil)
	require.NoError(t, err)

	err = col.AppendRow(map[string]any{"name": "hello", "value": uint64(42)})
	require.NoError(t, err)

	var result map[int]any
	err = col.ScanRow(&result, 0)
	require.EqualError(t, err, "int: column tuple - map keys must be a string")
}

func TestTupleParseElementNames(t *testing.T) {
	for _, tc := range []struct {
		chType  string
		names   []string
		types   []string
		isNamed bool
	}{
		{
			chType:  "Tuple(key String, values Array(String))",
			names:   []string{"key", "values"},
			types:   []string{"String", "Array(String)"},
			isNamed: true,
		},
		{
			// reserved words are backquoted by ClickHouse 26.5+
			chType:  "Tuple(key String, `values` Array(String), `from` UInt8)",
			names:   []string{"key", "values", "from"},
			types:   []string{"String", "Array(String)", "UInt8"},
			isNamed: true,
		},
		{
			// pretty type names as sent over HTTP
			chType:  "Tuple(\n    key String,\n    `values` Array(String))",
			names:   []string{"key", "values"},
			types:   []string{"String", "Array(String)"},
			isNamed: true,
		},
		{
			chType:  "Tuple(`56` String, `a22\\`` Int64)",
			names:   []string{"56", "a22`"},
			types:   []string{"String", "Int64"},
			isNamed: true,
		},
		{
			// a lone backtick is not a quoted name and must not panic
			chType:  "Tuple(` String)",
			names:   []string{"`"},
			types:   []string{"String"},
			isNamed: true,
		},
		{
			chType:  "Tuple(String, Decimal(9, 2))",
			names:   []string{"", ""},
			types:   []string{"String", "Decimal(9, 2)"},
			isNamed: false,
		},
		{
			// a quoted name can contain a space, a comma, and a bracket
			chType:  "Tuple(`a b` UInt8, `c,d` UInt8, `e(f` UInt8, `g\\`h` Tuple(`i j` UInt8))",
			names:   []string{"a b", "c,d", "e(f", "g`h"},
			types:   []string{"UInt8", "UInt8", "UInt8", "Tuple(`i j` UInt8)"},
			isNamed: true,
		},
		{
			// a string in a type can contain a comma and a bracket
			chType:  "Tuple(Enum8('x)' = 1, 'c,d' = 2), UInt8)",
			names:   []string{"", ""},
			types:   []string{"Enum8('x)' = 1, 'c,d' = 2)", "UInt8"},
			isNamed: false,
		},
		{
			chType:  "Tuple()",
			isNamed: false,
		},
	} {
		t.Run(tc.chType, func(t *testing.T) {
			col, err := Type(tc.chType).Column("tuple", nil)
			require.NoError(t, err)
			tuple := col.(*Tuple)
			require.Equal(t, tc.isNamed, tuple.isNamed)
			require.Len(t, tuple.columns, len(tc.names))
			for i := range tc.names {
				require.Equal(t, tc.names[i], tuple.columns[i].Name())
				require.Equal(t, tc.types[i], string(tuple.columns[i].Type()))
			}
		})
	}
}

func TestUnescapeColName(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{in: "", want: ""},
		{in: "`", want: "`"},
		{in: "``", want: ""},
		{in: "`values`", want: "values"},
		{in: "`a22\\``", want: "a22`"},
		{in: "values", want: "values"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			require.NotPanics(t, func() {
				require.Equal(t, tc.want, unescapeColName(tc.in), "unescapeColName(%q)", tc.in)
			})
		})
	}
}

func TestTupleBackquotedStructTags(t *testing.T) {
	// before element names were stored unquoted, a backquoted tag was the only way to match
	// names such as `56` from a struct, so it must keep working
	type element struct {
		Num  string "ch:\"`56`\""
		From string "json:\"`from`\""
	}
	col, err := Type("Tuple(`56` String, `from` String)").Column("tuple", nil)
	require.NoError(t, err)

	want := element{Num: "n", From: "x"}
	require.NoError(t, col.AppendRow(want))

	var got element
	require.NoError(t, col.ScanRow(&got, 0))
	require.Equal(t, want, got)
}

func TestTupleNestedQuotedElementNames(t *testing.T) {
	col, err := Type("Tuple(`from` Tuple(`values` String, id UInt8))").Column("tuple", nil)
	require.NoError(t, err)
	outer := col.(*Tuple)
	require.Equal(t, "from", outer.columns[0].Name())
	inner := outer.columns[0].(*Tuple)
	require.Equal(t, "values", inner.columns[0].Name())
	require.Equal(t, "id", inner.columns[1].Name())
}

func TestTupleQuotedElementNamesRoundTrip(t *testing.T) {
	type element struct {
		Values []string `ch:"values"`
		From   string   `ch:"from"`
		Num    uint8    `ch:"56"`
	}
	col, err := Type("Tuple(`values` Array(String), `from` String, `56` UInt8)").Column("tuple", nil)
	require.NoError(t, err)

	fromStruct := element{Values: []string{"a", "b"}, From: "x", Num: 1}
	require.NoError(t, col.AppendRow(fromStruct))
	fromMap := map[string]any{"values": []string{"c"}, "from": "y", "56": uint8(2)}
	require.NoError(t, col.AppendRow(fromMap))

	var gotStruct element
	require.NoError(t, col.ScanRow(&gotStruct, 0))
	require.Equal(t, fromStruct, gotStruct)

	var gotMap map[string]any
	require.NoError(t, col.ScanRow(&gotMap, 1))
	require.Equal(t, fromMap, gotMap)

	err = col.AppendRow(map[string]any{"values": []string{}, "from": "z", "missing": uint8(0)})
	require.ErrorContains(t, err, "sub column 'missing' does not exist")
}

func TestTupleNestedRow(t *testing.T) {
	for _, tc := range []struct {
		chType string
		row    any
	}{
		{
			chType: "Tuple(Tuple(UInt8, String), UInt8)",
			row:    []any{[]any{uint8(1), "a"}, uint8(2)},
		},
		{
			chType: "Tuple(UInt8, Tuple(UInt8, Tuple(UInt8, String)))",
			row:    []any{uint8(1), []any{uint8(2), []any{uint8(3), "a"}}},
		},
		{
			chType: "Tuple(a Tuple(UInt8, String), b UInt8)",
			row:    map[string]any{"a": []any{uint8(1), "a"}, "b": uint8(2)},
		},
		{
			chType: "Tuple(Tuple(a UInt8, b String), UInt8)",
			row:    []any{map[string]any{"a": uint8(1), "b": "a"}, uint8(2)},
		},
		{
			chType: "Array(Tuple(Tuple(UInt8, String), UInt8))",
			row:    [][]any{{[]any{uint8(1), "a"}, uint8(2)}},
		},
		{
			chType: "Tuple(Array(Tuple(UInt8, UInt8)), UInt8)",
			row:    []any{[][]any{{uint8(1), uint8(2)}}, uint8(3)},
		},
		{
			chType: "Tuple(a Array(Tuple(UInt8, UInt8)), b UInt8)",
			row:    map[string]any{"a": [][]any{{uint8(1), uint8(2)}}, "b": uint8(3)},
		},
		{
			chType: "Tuple(Array(Array(Tuple(a UInt8, b UInt8))), UInt8)",
			row: []any{[][]map[string]any{
				{{"a": uint8(1), "b": uint8(2)}},
				{{"a": uint8(3), "b": uint8(4)}, {"a": uint8(5), "b": uint8(6)}},
			}, uint8(7)},
		},
		{
			chType: "Tuple(Tuple(), Array(Tuple()))",
			row:    []any{[]any{}, [][]any{{}, {}}},
		},
	} {
		t.Run(tc.chType, func(t *testing.T) {
			col, err := Type(tc.chType).Column("test", nil)
			require.NoError(t, err)
			require.NoError(t, col.AppendRow(tc.row))
			assert.Equal(t, tc.row, col.Row(0, false))
			var scanned any
			require.NoError(t, col.ScanRow(&scanned, 0))
			assert.Equal(t, tc.row, scanned)
		})
	}
}

func TestTupleNestedScanTyped(t *testing.T) {
	col, err := Type("Tuple(a Tuple(UInt8, String), b Tuple(UInt8, String))").Column("test", nil)
	require.NoError(t, err)
	require.NoError(t, col.AppendRow(map[string]any{"a": []any{uint8(1), "a"}, "b": []any{uint8(2), "b"}}))

	var slices map[string][]any
	require.NoError(t, col.ScanRow(&slices, 0))
	assert.Equal(t, map[string][]any{"a": {uint8(1), "a"}, "b": {uint8(2), "b"}}, slices)

	var maps map[string]map[string]any
	require.ErrorContains(t, col.ScanRow(&maps, 0), "cannot use maps for unnamed tuples, use slice")

	var numbers map[string]uint8
	require.ErrorContains(t, col.ScanRow(&numbers, 0), "needs a map/struct/slice or any")

	var st struct {
		A []any `ch:"a"`
		B any   `ch:"b"`
	}
	require.NoError(t, col.ScanRow(&st, 0))
	assert.Equal(t, []any{uint8(1), "a"}, st.A)
	assert.Equal(t, []any{uint8(2), "b"}, st.B)
}

func TestTupleEmpty(t *testing.T) {
	col, err := Type("Tuple()").Column("test", nil)
	require.NoError(t, err)
	require.NoError(t, col.AppendRow([]any{}))
	require.NoError(t, col.AppendRow([]any{}))
	require.Error(t, col.AppendRow([]any{uint8(1)}))
	require.Equal(t, 2, col.Rows())
	assert.Equal(t, []any{}, col.Row(1, false))

	var buffer proto.Buffer
	col.Encode(&buffer)
	assert.Equal(t, []byte("00"), buffer.Buf)

	decoded, err := Type("Tuple()").Column("test", nil)
	require.NoError(t, err)
	require.NoError(t, decoded.Decode(buffer.Reader(), 2))
	require.Equal(t, 2, decoded.Rows())
	var scanned []any
	require.NoError(t, decoded.ScanRow(&scanned, 1))
	assert.Equal(t, []any{}, scanned)

	col.Reset()
	require.Equal(t, 0, col.Rows())
}
