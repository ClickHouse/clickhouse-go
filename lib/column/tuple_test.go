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

type keyValues struct {
	Key    string   `ch:"key"`
	Values []string `ch:"values"`
}

// ClickHouse 26.6+ reports keyword element names backquoted, e.g. `values`.
func TestTupleQuotedElementNames(t *testing.T) {
	for _, chType := range []Type{
		"Tuple(key String, values Array(String))",
		"Tuple(key String, `values` Array(String))",
	} {
		t.Run(string(chType), func(t *testing.T) {
			col, err := chType.Column("kv", nil)
			require.NoError(t, err)

			require.NoError(t, col.AppendRow(keyValues{Key: "env", Values: []string{"prod", "dev"}}))
			require.NoError(t, col.AppendRow(map[string]any{"key": "team", "values": []string{"core"}}))

			var asStruct keyValues
			require.NoError(t, col.ScanRow(&asStruct, 0))
			require.Equal(t, keyValues{Key: "env", Values: []string{"prod", "dev"}}, asStruct)

			var asMap map[string]any
			require.NoError(t, col.ScanRow(&asMap, 1))
			require.Equal(t, map[string]any{"key": "team", "values": []string{"core"}}, asMap)
		})
	}
}

func TestTupleEscapedElementNames(t *testing.T) {
	col, err := Type("Tuple(`56` String, `a22\\`` Int64)").Column("t", nil)
	require.NoError(t, err)

	require.NoError(t, col.AppendRow(map[string]any{"56": "A", "a22`": int64(1)}))

	var got map[string]any
	require.NoError(t, col.ScanRow(&got, 0))
	require.Equal(t, map[string]any{"56": "A", "a22`": int64(1)}, got)
}
