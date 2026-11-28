package column

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingUnmarshaler struct{ data []byte }

func (r *recordingUnmarshaler) UnmarshalBinary(data []byte) error {
	r.data = data
	return nil
}

func TestStringScanRowBytesAreCopies(t *testing.T) {
	col := &String{}
	col.col.Append("A")
	col.col.Append("A")
	col.col.Append("")

	scans := []struct {
		name string
		scan func(t *testing.T, row int) []byte
	}{
		{"*[]byte", func(t *testing.T, row int) []byte {
			var d []byte
			require.NoError(t, col.ScanRow(&d, row))
			return d
		}},
		{"**[]byte", func(t *testing.T, row int) []byte {
			var d *[]byte
			require.NoError(t, col.ScanRow(&d, row))
			return *d
		}},
		{"*json.RawMessage", func(t *testing.T, row int) []byte {
			var d json.RawMessage
			require.NoError(t, col.ScanRow(&d, row))
			return d
		}},
		{"**json.RawMessage", func(t *testing.T, row int) []byte {
			var d *json.RawMessage
			require.NoError(t, col.ScanRow(&d, row))
			return *d
		}},
		{"encoding.BinaryUnmarshaler", func(t *testing.T, row int) []byte {
			var d recordingUnmarshaler
			require.NoError(t, col.ScanRow(&d, row))
			return d.data
		}},
	}

	for _, tt := range scans {
		t.Run(tt.name, func(t *testing.T) {
			first, second := tt.scan(t, 0), tt.scan(t, 1)
			require.Equal(t, []byte("A"), first)
			require.Equal(t, []byte("A"), second)
			require.NotSame(t, &first[0], &second[0], "values scanned from different rows share memory")

			first[0] = 'a'
			assert.Equal(t, []byte("a"), first)
			assert.Equal(t, []byte("A"), second)

			assert.Nil(t, tt.scan(t, 2), "an empty value should stay nil")
		})
	}
}
