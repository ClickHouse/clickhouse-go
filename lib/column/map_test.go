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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column, err := tt.columnType.Column("test", &ServerContext{})
			require.NoError(t, err)

			m := requireMap(t, column)
			assert.Equal(t, tt.keyType, m.keys.Type())
			assert.Equal(t, tt.valueType, m.values.Type())
		})
	}
}

func TestMapParseInvalid(t *testing.T) {
	_, err := Type("Map(String)").Column("test", &ServerContext{})
	require.Error(t, err)
	assert.IsType(t, &UnsupportedColumnTypeError{}, err)
}

func requireMap(t *testing.T, column Interface) *Map {
	t.Helper()
	m, ok := column.(*Map)
	require.True(t, ok)
	return m
}
