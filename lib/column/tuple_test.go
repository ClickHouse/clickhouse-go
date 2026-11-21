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
			chType:  "Tuple(`56` String, `a22\\`` Int64)",
			names:   []string{"56", "a22`"},
			types:   []string{"String", "Int64"},
			isNamed: true,
		},
		{
			chType:  "Tuple(`a b` Int64, `c\\` d` Decimal(10, 2), e String)",
			names:   []string{"a b", "c` d", "e"},
			types:   []string{"Int64", "Decimal(10, 2)", "String"},
			isNamed: true,
		},
		{
			// parentheses and commas in a backquoted name belong to the name
			chType:  "Tuple(`price (usd)` Float64, `a, b` Int64, `x (y` Int8, `z)` Int8, e String)",
			names:   []string{"price (usd)", "a, b", "x (y", "z)", "e"},
			types:   []string{"Float64", "Int64", "Int8", "Int8", "String"},
			isNamed: true,
		},
		{
			// a backtick inside an enum value is not a name
			chType:  "Tuple(a Enum8('x`y' = 1, 'z' = 2), `b c` Int64)",
			names:   []string{"a", "b c"},
			types:   []string{"Enum8('x`y' = 1, 'z' = 2)", "Int64"},
			isNamed: true,
		},
		{
			// a parenthesis inside an enum value does not end the element
			chType:  "Tuple(a Enum8('x)y' = 1, 'z' = 2), b Int8)",
			names:   []string{"a", "b"},
			types:   []string{"Enum8('x)y' = 1, 'z' = 2)", "Int8"},
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
			// a newline or tab may separate a name from its type
			chType:  "Tuple(`a b`\nDecimal(10, 2), `c`\tInt64, d\tString, e\n    UInt8)",
			names:   []string{"a b", "c", "d", "e"},
			types:   []string{"Decimal(10, 2)", "Int64", "String", "UInt8"},
			isNamed: true,
		},
		{
			// the type may follow the closing quote directly
			chType:  "Tuple(`a b`Int64, \"c d\"String)",
			names:   []string{"a b", "c d"},
			types:   []string{"Int64", "String"},
			isNamed: true,
		},
		{
			// double-quoted names: plain, with an escaped quote, with a comma and parentheses
			chType:  `Tuple("a b" Int64, "c\"d" String, "e, (f)" Int8)`,
			names:   []string{"a b", `c"d`, "e, (f)"},
			types:   []string{"Int64", "String", "Int8"},
			isNamed: true,
		},
		{
			// escaped backslashes, on the wire `a\\`, `b\\\`` and "c\\": the names a\, b\` and c\
			chType:  "Tuple(`a\\\\` Int64, `b\\\\\\`` String, \"c\\\\\" UInt8)",
			names:   []string{`a\`, "b\\`", `c\`},
			types:   []string{"Int64", "String", "UInt8"},
			isNamed: true,
		},
		{
			// the other quote inside a quoted name is plain text
			chType:  "Tuple(`a\"b` Int8, \"c`d\" String)",
			names:   []string{`a"b`, "c`d"},
			types:   []string{"Int8", "String"},
			isNamed: true,
		},
		{
			// the server writes control characters in a name as \t, \n, \r, \0, \b and \f
			chType:  "Tuple(`a\\tb` Int64, `c\\nd` String, `e\\rf\\0g\\bh\\fi` UInt8)",
			names:   []string{"a\tb", "c\nd", "e\rf\x00g\bh\fi"},
			types:   []string{"Int64", "String", "UInt8"},
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
