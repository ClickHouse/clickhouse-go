package column

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQBit_Parse(t *testing.T) {
	tests := []struct {
		name          string
		colType       Type
		wantElement   string
		wantDimension int
		wantErr       bool
	}{
		{
			name:          "Float32 valid",
			colType:       "QBit(Float32, 1024)",
			wantElement:   "Float32",
			wantDimension: 1024,
			wantErr:       false,
		},
		{
			name:          "Float64 valid",
			colType:       "QBit(Float64, 512)",
			wantElement:   "Float64",
			wantDimension: 512,
			wantErr:       false,
		},
		{
			name:          "BFloat16 valid",
			colType:       "QBit(BFloat16, 256)",
			wantElement:   "BFloat16",
			wantDimension: 256,
			wantErr:       false,
		},
		{
			name:    "Invalid element type",
			colType: "QBit(Int32, 128)",
			wantErr: true,
		},
		{
			name:    "Invalid format",
			colType: "QBit(Float32)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &QBit{name: "test"}
			result, err := col.parse(tt.colType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantElement, result.elementType)
				assert.Equal(t, tt.wantDimension, result.dimension)
				assert.Equal(t, tt.colType, result.Type())
			}
		})
	}
}

func TestQBit_AppendRow_Float32(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	// Test []float32
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	err = col.AppendRow(vec1)
	require.NoError(t, err)
	assert.Equal(t, 1, col.Rows())

	// Test *[]float32
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	err = col.AppendRow(&vec2)
	require.NoError(t, err)
	assert.Equal(t, 2, col.Rows())

	// Test nil
	err = col.AppendRow(nil)
	require.NoError(t, err)
	assert.Equal(t, 3, col.Rows())

	// Verify values
	result0 := col.row(0)
	assert.Equal(t, vec1, result0)

	result1 := col.row(1)
	assert.Equal(t, vec2, result1)

	result2 := col.row(2)
	assert.Equal(t, []float32{0, 0, 0, 0}, result2)
}

func TestQBit_AppendRow_Float64(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 3)")
	require.NoError(t, err)

	// Test []float64
	vec1 := []float64{1.5, 2.5, 3.5}
	err = col.AppendRow(vec1)
	require.NoError(t, err)

	// Test *[]float64
	vec2 := []float64{4.5, 5.5, 6.5}
	err = col.AppendRow(&vec2)
	require.NoError(t, err)

	// Verify values (converted to float32)
	result0 := col.row(0)
	assert.InDeltaSlice(t, []float32{1.5, 2.5, 3.5}, result0, 1e-6)

	result1 := col.row(1)
	assert.InDeltaSlice(t, []float32{4.5, 5.5, 6.5}, result1, 1e-6)
}

func TestQBit_AppendRow_WithNilElements(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 3)")
	require.NoError(t, err)

	// Test []*float32 with nil elements
	val1 := float32(1.0)
	val3 := float32(3.0)
	vec := []*float32{&val1, nil, &val3}
	err = col.AppendRow(vec)
	require.NoError(t, err)

	result := col.row(0)
	assert.Equal(t, []float32{1.0, 0.0, 3.0}, result)
}

func TestQBit_Append_MultipleVectors(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	// Test [][]float32
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}
	_, err = col.Append(vectors)
	require.NoError(t, err)
	assert.Equal(t, 3, col.Rows())

	// Verify all vectors
	for i, expected := range vectors {
		result := col.row(i)
		assert.Equal(t, expected, result, "vector %d", i)
	}
}

func TestQBit_Append_Float64Vectors(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 2)")
	require.NoError(t, err)

	// Test [][]float64
	vectors := [][]float64{
		{1.5, 2.5},
		{3.5, 4.5},
	}
	_, err = col.Append(vectors)
	require.NoError(t, err)
	assert.Equal(t, 2, col.Rows())

	// Verify conversion to float32
	result0 := col.row(0)
	assert.InDeltaSlice(t, []float32{1.5, 2.5}, result0, 1e-6)
}

func TestQBit_ScanRow_Float32(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	vec := []float32{1.0, 2.0, 3.0, 4.0}
	err = col.AppendRow(vec)
	require.NoError(t, err)

	// Test scan to *[]float32
	var result1 []float32
	err = col.ScanRow(&result1, 0)
	require.NoError(t, err)
	assert.Equal(t, vec, result1)

	// Test scan to **[]float32
	var result2 *[]float32
	err = col.ScanRow(&result2, 0)
	require.NoError(t, err)
	require.NotNil(t, result2)
	assert.Equal(t, vec, *result2)
}

func TestQBit_ScanRow_Float64(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 3)")
	require.NoError(t, err)

	vec := []float32{1.5, 2.5, 3.5}
	err = col.AppendRow(vec)
	require.NoError(t, err)

	// Test scan to *[]float64
	var result []float64
	err = col.ScanRow(&result, 0)
	require.NoError(t, err)
	assert.InDeltaSlice(t, []float64{1.5, 2.5, 3.5}, result, 1e-6)
}

func TestQBit_EncodeDecodeRoundtrip(t *testing.T) {
	// Create and populate column
	col1 := &QBit{name: "test"}
	_, err := col1.parse("QBit(Float32, 8)")
	require.NoError(t, err)

	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0},
		{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0},
		{-1.5, -2.5, -3.5, -4.5, -5.5, -6.5, -7.5, -8.5},
	}

	for _, vec := range vectors {
		err = col1.AppendRow(vec)
		require.NoError(t, err)
	}

	// Encode
	var buf proto.Buffer
	col1.Encode(&buf)

	// Decode
	col2 := &QBit{name: "test"}
	_, err = col2.parse("QBit(Float32, 8)")
	require.NoError(t, err)

	reader := proto.NewReader(bytes.NewReader(buf.Buf))
	err = col2.Decode(reader, 3)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, 3, col2.Rows())
	for i, expected := range vectors {
		result := col2.row(i)
		assert.Equal(t, expected, result, "vector %d", i)
	}
}

func TestQBit_Reset(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	// Add data
	err = col.AppendRow([]float32{1.0, 2.0, 3.0, 4.0})
	require.NoError(t, err)
	err = col.AppendRow([]float32{5.0, 6.0, 7.0, 8.0})
	require.NoError(t, err)
	assert.Equal(t, 2, col.Rows())

	// Reset
	col.Reset()
	assert.Equal(t, 0, col.Rows())

	// Can append after reset
	err = col.AppendRow([]float32{10.0, 20.0, 30.0, 40.0})
	require.NoError(t, err)
	assert.Equal(t, 1, col.Rows())
}

func TestQBit_Type_And_Name(t *testing.T) {
	col := &QBit{name: "embedding"}
	_, err := col.parse("QBit(Float32, 1024)")
	require.NoError(t, err)

	assert.Equal(t, "embedding", col.Name())
	assert.Equal(t, Type("QBit(Float32, 1024)"), col.Type())
}

func TestQBit_ScanType(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 128)")
	require.NoError(t, err)

	scanType := col.ScanType()
	assert.Equal(t, "[]float32", scanType.String())
}

func TestQBit_AppendRow_InvalidType(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	// Try to append invalid type
	err = col.AppendRow("invalid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "QBit")
}

func TestQBit_AppendRow_WrongDimension(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 4)")
	require.NoError(t, err)

	// Try to append vector with wrong dimension
	wrongVec := []float32{1.0, 2.0, 3.0} // Only 3 elements, expected 4
	err = col.AppendRow(wrongVec)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func TestQBit_BFloat16_Precision(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(BFloat16, 4)")
	require.NoError(t, err)

	// BFloat16 has reduced precision
	vec := []float32{1.5, 2.25, 3.125, 4.0625}
	err = col.AppendRow(vec)
	require.NoError(t, err)

	result := col.row(0)
	// BFloat16 precision check - should be close but may have rounding
	for i := range vec {
		assert.InDelta(t, vec[i], result[i], 0.01, "element %d", i)
	}
}

func TestQBit_LargeVectors(t *testing.T) {
	// Test with real-world embedding dimensions
	dimensions := []int{128, 256, 512, 768, 1024, 1536}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			col := &QBit{name: "test"}
			_, err := col.parse(Type(fmt.Sprintf("QBit(Float32, %d)", dim)))
			require.NoError(t, err)

			// Create test vector
			vec := make([]float32, dim)
			for i := range vec {
				vec[i] = float32(i) / float32(dim)
			}

			err = col.AppendRow(vec)
			require.NoError(t, err)

			result := col.row(0)
			assert.Equal(t, dim, len(result))
			assert.Equal(t, vec, result)
		})
	}
}

func TestQBit_Row_Method(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 3)")
	require.NoError(t, err)

	vec := []float32{1.0, 2.0, 3.0}
	err = col.AppendRow(vec)
	require.NoError(t, err)

	// Test Row method with ptr=false
	result := col.Row(0, false)
	resultSlice, ok := result.([]float32)
	require.True(t, ok)
	assert.Equal(t, vec, resultSlice)

	// Test Row method with ptr=true
	resultPtr := col.Row(0, true)
	resultSlicePtr, ok := resultPtr.(*[]float32)
	require.True(t, ok)
	assert.Equal(t, vec, *resultSlicePtr)
}

func TestQBit_Append_NullableVectors(t *testing.T) {
	col := &QBit{name: "test"}
	_, err := col.parse("QBit(Float32, 2)")
	require.NoError(t, err)

	// Test [][]*float32 with nil vectors
	val1 := float32(1.0)
	val2 := float32(2.0)
	val3 := float32(3.0)
	vectors := [][]*float32{
		{&val1, &val2},
		nil,          // nil vector
		{&val3, nil}, // vector with nil element
	}

	nulls, err := col.Append(vectors)
	require.NoError(t, err)
	assert.Equal(t, []uint8{0, 1, 0}, nulls)
	assert.Equal(t, 3, col.Rows())

	// Verify first vector
	result0 := col.row(0)
	assert.Equal(t, []float32{1.0, 2.0}, result0)

	// Verify nil vector (should be zeros)
	result1 := col.row(1)
	assert.Equal(t, []float32{0.0, 0.0}, result1)

	// Verify vector with nil element
	result2 := col.row(2)
	assert.Equal(t, []float32{3.0, 0.0}, result2)
}

// Benchmark tests
func BenchmarkQBit_AppendRow_1536(b *testing.B) {
	col := &QBit{name: "test"}
	col.parse("QBit(Float32, 1536)")
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.AppendRow(vec)
	}
}

func BenchmarkQBit_ScanRow_1536(b *testing.B) {
	col := &QBit{name: "test"}
	col.parse("QBit(Float32, 1536)")
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i)
	}
	col.AppendRow(vec)

	var result []float32
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.ScanRow(&result, 0)
	}
}
