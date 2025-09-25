package column

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An implementation of FixedString column
type testBinaryFixedString struct {
	data []byte
}

func (t *testBinaryFixedString) MarshalBinary() ([]byte, error) {
	return t.data, nil
}

func TestFixedStringAppendBinaryMarshaler(t *testing.T) {
	tests := []struct {
		name          string
		inputSize     int
		data          []byte
		expectedNulls []uint8
		expectedSize  int
	}{
		{
			name:          "empty-size-empty-data",
			inputSize:     0,
			data:          []byte{},
			expectedNulls: []uint8{},
			expectedSize:  0,
		},
		{
			name:          "empty-size-non-empty-data",
			inputSize:     0,
			data:          []byte("test"),
			expectedNulls: []uint8{0},
			expectedSize:  4,
		},
		{
			name:          "happy-path",
			inputSize:     4,
			data:          []byte("test"),
			expectedNulls: []uint8{0},
			expectedSize:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &FixedString{
				name: "test",
				col: proto.ColFixedStr{
					Size: tt.inputSize,
				},
			}

			binData := &testBinaryFixedString{
				data: tt.data,
			}

			nulls, err := col.Append(binData)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedNulls, nulls)
			assert.Equal(t, tt.expectedSize, col.col.Size)
		})
	}
}

