package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColVariant_parse(t *testing.T) {
	const escapedTuple = "Tuple(\"c\\\"d\" String, `a\\\\` Int64, `b\\\\\\`` Int8, `a\"b` UInt8, \"c`d\" String)"
	const formattedTuple = "Tuple(\n    `a b`\tInt8,\n    \"c d\"String)"
	const controlTuple = "Tuple(`a\\tb` Int64, `c\\nd` String, `e\\rf\\0g\\bh\\fi` UInt8)"
	cases := []struct {
		typ           Type
		expectedTypes []Type
	}{
		{typ: "Variant(Int64)", expectedTypes: []Type{"Int64"}},
		{typ: "Variant(Int64, String)", expectedTypes: []Type{"Int64", "String"}},
		{typ: "Variant(Array(String), Int64, String)", expectedTypes: []Type{"Array(String)", "Int64", "String"}},
		{typ: "Variant(Array(Map(String, String)), Map(String, Int64))", expectedTypes: []Type{"Array(Map(String, String))", "Map(String, Int64)"}},
		{typ: "Variant(Array(Map(String, Tuple(a String, b Int64))), Map(String, Int64))", expectedTypes: []Type{"Array(Map(String, Tuple(a String, b Int64)))", "Map(String, Int64)"}},
		// Issue #2073: quoted delimiters must not hide or split Variant members.
		{typ: "Variant(Tuple(`z)` Int8), String)", expectedTypes: []Type{"Tuple(`z)` Int8)", "String"}},
		{typ: "Variant(Tuple(\"x(y\" Int8), String)", expectedTypes: []Type{"Tuple(\"x(y\" Int8)", "String"}},
		{typ: "Variant(Tuple(`a, b` Int8, `c\\` d` String), String)", expectedTypes: []Type{"Tuple(`a, b` Int8, `c\\` d` String)", "String"}},
		{typ: "Variant(Enum8('x)y' = 1, 'z' = 2), String)", expectedTypes: []Type{"Enum8('x)y' = 1, 'z' = 2)", "String"}},
		{typ: Type("Variant(" + escapedTuple + ", UInt64)"), expectedTypes: []Type{escapedTuple, "UInt64"}},
		{typ: Type("Variant(\n\t" + formattedTuple + ",\n\tUInt64)"), expectedTypes: []Type{formattedTuple, "UInt64"}},
		{typ: Type("Variant(" + controlTuple + ", UInt64)"), expectedTypes: []Type{controlTuple, "UInt64"}},
		{typ: "Variant(Array(Tuple(`a, b` Int8, `z)` String)), UInt64)", expectedTypes: []Type{"Array(Tuple(`a, b` Int8, `z)` String))", "UInt64"}},
		{typ: "Variant(Map(String, Tuple(`x(y` Int8, `c\\` d` String)), UInt64)", expectedTypes: []Type{"Map(String, Tuple(`x(y` Int8, `c\\` d` String))", "UInt64"}},
	}

	for i, c := range cases {
		t.Run(string(c.typ), func(t *testing.T) {
			col, err := (&Variant{name: "vt"}).parse(c.typ, nil)
			require.NoError(t, err, "case index %d failed to parse Variant column", i)

			require.Equal(t, "vt", col.Name())
			require.Equal(t, c.typ, col.chType)
			require.Equal(t, len(c.expectedTypes), len(col.columns))
			require.Equal(t, len(c.expectedTypes), len(col.columnTypeIndex))

			for j, subCol := range col.columns {
				expectedType := c.expectedTypes[j]
				actualType := subCol.Type()
				assert.Equal(t, expectedType, actualType, "case index %d Variant type index %d column type does not match", i, j)

				expectedColumnTypeIndex := uint8(j)
				actualColumnTypeIndex := col.columnTypeIndex[string(actualType)]
				assert.Equal(t, expectedColumnTypeIndex, actualColumnTypeIndex, "case index %d Variant type index %d columnTypeIndex does not match", i, j)
			}
		})
	}
}

func TestColVariant_parse_invalid(t *testing.T) {
	cases := []Type{
		"",
		"Variant",
		"Variant(Array(Map(String)), Map(String, Int64))",
		"Variant(Array(Tuple(String, b Int64)), Map(String, Int64), FakeType)",
	}

	for i, typeName := range cases {
		_, err := (&Variant{name: "vt"}).parse(typeName, nil)
		require.Error(t, err, "expected error for case index %d (\"%s\"), but received nil", i, typeName)
	}
}

func TestColVariant_addColumn(t *testing.T) {
	col := Variant{columnTypeIndex: make(map[string]uint8, 1)}

	col.addColumn(&Int64{})

	require.Equal(t, 1, len(col.columns))
	require.Equal(t, 1, len(col.columnTypeIndex))
	require.Equal(t, Type("Int64"), col.columns[0].Type())
	require.Equal(t, uint8(0), col.columnTypeIndex["Int64"])
}

func TestColVariant_appendDiscriminatorRow(t *testing.T) {
	col := Variant{}
	var discriminator uint8 = 8

	col.appendDiscriminatorRow(discriminator)

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, discriminator, col.discriminators[0])
}

func TestColVariant_appendNullRow(t *testing.T) {
	col := Variant{}

	col.appendNullRow()

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, VariantNullDiscriminator, col.discriminators[0])
}
