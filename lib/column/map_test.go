package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapParse(t *testing.T) {
	tests := []struct {
		name       string
		columnType Type
		keyType    Type
		valueType  Type
		tupleNames []string
		tupleTypes []Type
		variants   []Type
	}{
		{
			name:       "simple types",
			columnType: "Map(String, UInt64)",
			keyType:    "String",
			valueType:  "UInt64",
		},
		{
			name:       "enum key",
			columnType: "Map(Enum16('one' = 1, 'two' = 2), UInt64)",
			keyType:    "Enum16('one' = 1, 'two' = 2)",
			valueType:  "UInt64",
		},
		{
			name:       "enum key containing parenthesis",
			columnType: "Map(Enum8(')' = 1, 'other' = 2), UInt64)",
			keyType:    "Enum8(')' = 1, 'other' = 2)",
			valueType:  "UInt64",
		},
		{
			name:       "parameterized key containing comma",
			columnType: "Map(DateTime64(3, 'UTC'), String)",
			keyType:    "DateTime64(3, 'UTC')",
			valueType:  "String",
		},
		{
			name:       "nested value containing comma",
			columnType: "Map(String, Tuple(UInt8, UInt16))",
			keyType:    "String",
			valueType:  "Tuple(UInt8, UInt16)",
		},
		{
			name:       "quoted tuple value from issue 2073",
			columnType: "Map(String, Tuple(`a, b` Int8, `z)` String))",
			keyType:    "String",
			valueType:  "Tuple(`a, b` Int8, `z)` String)",
			tupleNames: []string{"a, b", "z)"},
			tupleTypes: []Type{"Int8", "String"},
		},
		{
			name:       "variant containing quoted tuple value",
			columnType: "Map(String, Variant(Tuple(\"x(y\" Int8, \"c\\\"d\" String), UInt64))",
			keyType:    "String",
			valueType:  "Variant(Tuple(\"x(y\" Int8, \"c\\\"d\" String), UInt64)",
			tupleNames: []string{"x(y", "c\"d"},
			tupleTypes: []Type{"Int8", "String"},
			variants:   []Type{"Tuple(\"x(y\" Int8, \"c\\\"d\" String)", "UInt64"},
		},
		{
			name:       "enum key containing backtick",
			columnType: "Map(Enum8('x`y' = 1, 'other' = 2), UInt64)",
			keyType:    "Enum8('x`y' = 1, 'other' = 2)",
			valueType:  "UInt64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column, err := tt.columnType.Column("test", &ServerContext{})
			require.NoError(t, err)

			m := requireMap(t, column)
			assert.Equal(t, tt.keyType, m.keys.Type())
			assert.Equal(t, tt.valueType, m.values.Type())
			value := m.values
			if len(tt.variants) > 0 {
				variant := value.(*Variant)
				require.Len(t, variant.columns, len(tt.variants))
				for i, typ := range tt.variants {
					require.Equal(t, typ, variant.columns[i].Type())
					require.Equal(t, uint8(i), variant.columnTypeIndex[string(typ)])
				}
				value = variant.columns[0]
			}
			if len(tt.tupleNames) > 0 {
				tuple := value.(*Tuple)
				require.Len(t, tuple.columns, len(tt.tupleNames))
				for i, name := range tt.tupleNames {
					require.Equal(t, name, tuple.columns[i].Name())
					require.Equal(t, tt.tupleTypes[i], tuple.columns[i].Type())
				}
			}
		})
	}
}

func TestMapParseInvalid(t *testing.T) {
	for _, columnType := range []Type{
		"Map(String)",
		"Map(String,)",
		"Map(,String)",
		"Map(Array(String), UInt8)",
	} {
		_, err := columnType.Column("test", &ServerContext{})
		require.Error(t, err, string(columnType))
		assert.IsType(t, &UnsupportedColumnTypeError{}, err, string(columnType))
	}
}

func requireMap(t *testing.T, column Interface) *Map {
	t.Helper()
	m, ok := column.(*Map)
	require.True(t, ok)
	return m
}
