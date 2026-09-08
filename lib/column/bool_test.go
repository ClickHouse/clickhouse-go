package column

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoolAppendNullBool(t *testing.T) {
	valid := &sql.NullBool{Bool: true, Valid: true}
	invalid := &sql.NullBool{Bool: true, Valid: false}

	tests := []struct {
		name  string
		input any
		nulls []uint8
		rows  []bool
	}{
		{
			name: "values",
			input: []sql.NullBool{
				{Bool: true, Valid: true},
				{Bool: true, Valid: false},
				{Bool: false, Valid: true},
			},
			nulls: []uint8{0, 1, 0},
			rows:  []bool{true, false, false},
		},
		{
			name:  "pointers",
			input: []*sql.NullBool{valid, invalid, nil},
			nulls: []uint8{0, 1, 1},
			rows:  []bool{true, false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Bool{name: "test"}

			nulls, err := col.Append(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.nulls, nulls)
			require.Equal(t, len(tt.rows), col.Rows())
			for i, expected := range tt.rows {
				assert.Equal(t, expected, col.Row(i, false))
			}
		})
	}
}

func TestNullableBoolAppendNullBool(t *testing.T) {
	col := &Nullable{
		base:   &Bool{name: "test"},
		enable: true,
		name:   "test",
	}

	nulls, err := col.Append([]sql.NullBool{
		{Bool: true, Valid: true},
		{Bool: true, Valid: false},
	})
	require.NoError(t, err)
	assert.Equal(t, []uint8{0, 1}, nulls)
	assert.Equal(t, 2, col.Rows())
	assert.Equal(t, 2, col.base.Rows())
	assert.Equal(t, true, *col.Row(0, false).(*bool))
	assert.Nil(t, col.Row(1, false))
}
