package column

import (
	"testing"

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
			// separators inside a quoted name belong to the name
			chType:  "Tuple(`with space` String, `a, b` UInt8, `c(d)` UInt16)",
			names:   []string{"with space", "a, b", "c(d)"},
			types:   []string{"String", "UInt8", "UInt16"},
			isNamed: true,
		},
		{
			chType:  "Tuple(`56` String, `a22\\`` Int64, `back\\\\slash` UInt8)",
			names:   []string{"56", "a22`", "back\\slash"},
			types:   []string{"String", "Int64", "UInt8"},
			isNamed: true,
		},
		{
			chType:  "Tuple(e Enum8('a' = 1, 'b`c' = 2), n UInt8)",
			names:   []string{"e", "n"},
			types:   []string{"Enum8('a' = 1, 'b`c' = 2)", "UInt8"},
			isNamed: true,
		},
		{
			chType:  "Tuple(String, Decimal(9, 2))",
			names:   []string{"", ""},
			types:   []string{"String", "Decimal(9, 2)"},
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
		Spaced uint8    `ch:"with space"`
	}
	col, err := Type("Tuple(`values` Array(String), `from` String, `with space` UInt8)").Column("tuple", nil)
	require.NoError(t, err)

	fromStruct := element{Values: []string{"a", "b"}, From: "x", Spaced: 1}
	require.NoError(t, col.AppendRow(fromStruct))
	fromMap := map[string]any{"values": []string{"c"}, "from": "y", "with space": uint8(2)}
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
