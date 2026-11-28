package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrayStringScanRow checks that the []string fast path returns the same rows as
// the reflection path, which scans into any.
func TestArrayStringScanRow(t *testing.T) {
	col, err := Type("Array(String)").Column("test", &ServerContext{})
	require.NoError(t, err)
	rows := [][]string{{"a", "bb"}, {}, {"", "ccc", "d"}, {"e"}}
	for _, row := range rows {
		require.NoError(t, col.AppendRow(row))
	}

	for i, want := range rows {
		var got []string
		require.NoError(t, col.ScanRow(&got, i))
		assert.Equal(t, want, got)
		assert.NotNil(t, got)

		var viaReflection any
		require.NoError(t, col.ScanRow(&viaReflection, i))
		assert.Equal(t, viaReflection, any(got))
	}
}
