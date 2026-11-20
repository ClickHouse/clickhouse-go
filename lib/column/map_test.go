package column

import (
	"reflect"
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

func TestMapOrdered(t *testing.T) {
	sc := &ServerContext{OrderedMaps: true}
	entries := []MapEntry{{Key: "b", Value: uint64(1)}, {Key: "a", Value: uint64(2)}, {Key: "b", Value: uint64(3)}}

	col, err := Type("Map(String, UInt64)").Column("test", sc)
	require.NoError(t, err)
	assert.Equal(t, reflect.TypeFor[[]MapEntry](), col.ScanType())
	require.NoError(t, col.AppendRow(entries))
	require.NoError(t, col.AppendRow(nil))
	require.NoError(t, col.AppendRow(map[string]uint64{"c": 4}))
	assert.Equal(t, entries, col.Row(0, false))
	assert.Equal(t, []MapEntry{}, col.Row(1, false))
	assert.Equal(t, []MapEntry{{Key: "c", Value: uint64(4)}}, col.Row(2, false))

	var scanned []MapEntry
	require.NoError(t, col.ScanRow(&scanned, 0))
	assert.Equal(t, entries, scanned)
	var goMap map[string]uint64
	require.NoError(t, col.ScanRow(&goMap, 0))
	assert.Equal(t, map[string]uint64{"a": 2, "b": 3}, goMap)
}

func TestMapOrderedKeys(t *testing.T) {
	sc := &ServerContext{OrderedMaps: true}
	col, err := Type("Map(Array(String), UInt8)").Column("test", sc)
	require.NoError(t, err)
	entries := []MapEntry{{Key: []string{"x", "y"}, Value: uint8(1)}, {Key: []string{"x", "y"}, Value: uint8(2)}}
	require.NoError(t, col.AppendRow(entries))
	assert.Equal(t, entries, col.Row(0, false))
	var goMap map[string]uint8
	assert.Error(t, col.ScanRow(&goMap, 0))
}

func TestMapOrderedNested(t *testing.T) {
	sc := &ServerContext{OrderedMaps: true}
	col, err := Type("Array(Map(String, UInt8))").Column("test", sc)
	require.NoError(t, err)
	rows := [][]MapEntry{{{Key: "y", Value: uint8(1)}, {Key: "y", Value: uint8(2)}}, {}}
	require.NoError(t, col.AppendRow(rows))
	assert.Equal(t, rows, col.Row(0, false))

	col, err = Type("Map(String, Map(String, UInt8))").Column("test", sc)
	require.NoError(t, err)
	inner := []MapEntry{{Key: "k", Value: uint8(1)}, {Key: "k", Value: uint8(2)}}
	outer := []MapEntry{{Key: "a", Value: inner}, {Key: "a", Value: []MapEntry{}}}
	require.NoError(t, col.AppendRow(outer))
	assert.Equal(t, outer, col.Row(0, false))
}
