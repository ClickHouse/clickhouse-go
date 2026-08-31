package column

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDate32StringBounds(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{
			name:  "minimum date",
			value: "1900-01-01",
		},
		{
			name:    "before minimum date",
			value:   "1899-12-31",
			wantErr: true,
		},
		{
			name:  "maximum date",
			value: "2299-12-31",
		},
		{
			name:    "after maximum date",
			value:   "2300-01-01",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Date32{}

			err := col.AppendRow(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				require.Zero(t, col.Rows())
				return
			}

			require.NoError(t, err)
			require.Equal(t, 1, col.Rows())
		})
	}
}
