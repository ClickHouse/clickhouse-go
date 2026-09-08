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
