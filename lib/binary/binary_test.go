package binary

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testErrorReadWriter struct{}

func (*testErrorReadWriter) Read([]byte) (int, error)  { return 0, io.EOF }
func (*testErrorReadWriter) Write([]byte) (int, error) { return 0, io.EOF }

func Test_Uvarint(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := 1; i < 1000000000000000; i *= 42 {
		if err := encoder.Uvarint(uint64(i)); assert.NoError(t, err) {
			if v, err := decoder.Uvarint(); assert.NoError(t, err) {
				assert.Equal(t, uint64(i), v)
			}
		}
	}

	if err := encoder.Uvarint(math.MaxUint64); assert.NoError(t, err) {
		if v, err := decoder.Uvarint(); assert.NoError(t, err) {
			assert.Equal(t, uint64(math.MaxUint64), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Uvarint(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Uvarint(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, uint64(0), v)
		}
	}
}

func Test_Boolean(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	if err := encoder.Bool(false); assert.NoError(t, err) {
		if v, err := decoder.Bool(); assert.NoError(t, err) {
			assert.False(t, v)
		}
	}

	if err := encoder.Bool(true); assert.NoError(t, err) {
		if v, err := decoder.Bool(); assert.NoError(t, err) {
			assert.True(t, v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Bool(true); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Bool(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, false, v)
		}
	}
}

func Test_Int8(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -128; i <= 127; i++ {
		if err := encoder.Int8(int8(i)); assert.NoError(t, err) {
			if v, err := decoder.Int8(); assert.NoError(t, err) {
				assert.Equal(t, int8(i), v)
			}
		}
	}

	if err := encoder.Int8(math.MinInt8); assert.NoError(t, err) {
		if v, err := decoder.Int8(); assert.NoError(t, err) {
			assert.Equal(t, int8(math.MinInt8), v)
		}
	}

	if err := encoder.Int8(math.MaxInt8); assert.NoError(t, err) {
		if v, err := decoder.Int8(); assert.NoError(t, err) {
			assert.Equal(t, int8(math.MaxInt8), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Int8(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Int8(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, int8(0), v)
		}
	}
}

func Test_Int16(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -32768; i <= 32767; i += 10 {
		if err := encoder.Int16(int16(i)); assert.NoError(t, err) {
			if v, err := decoder.Int16(); assert.NoError(t, err) {
				assert.Equal(t, int16(i), v)
			}
		}
	}

	if err := encoder.Int16(math.MinInt16); assert.NoError(t, err) {
		if v, err := decoder.Int16(); assert.NoError(t, err) {
			assert.Equal(t, int16(math.MinInt16), v)
		}
	}

	if err := encoder.Int16(math.MaxInt16); assert.NoError(t, err) {
		if v, err := decoder.Int16(); assert.NoError(t, err) {
			assert.Equal(t, int16(math.MaxInt16), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Int16(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Int16(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, int16(0), v)
		}
	}
}
func Test_Int32(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -2147483648; i <= 2147483647; i += 100000 {
		if err := encoder.Int32(int32(i)); assert.NoError(t, err) {
			if v, err := decoder.Int32(); assert.NoError(t, err) {
				assert.Equal(t, int32(i), v)
			}
		}
	}

	if err := encoder.Int32(math.MinInt32); assert.NoError(t, err) {
		if v, err := decoder.Int32(); assert.NoError(t, err) {
			assert.Equal(t, int32(math.MinInt32), v)
		}
	}

	if err := encoder.Int32(math.MaxInt32); assert.NoError(t, err) {
		if v, err := decoder.Int32(); assert.NoError(t, err) {
			assert.Equal(t, int32(math.MaxInt32), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Int32(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Int32(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, int32(0), v)
		}
	}
}

func Test_Int64(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -2147483648; i <= 2147483647*2; i += 100000 {
		if err := encoder.Int64(int64(i)); assert.NoError(t, err) {
			if v, err := decoder.Int64(); assert.NoError(t, err) {
				assert.Equal(t, int64(i), v)
			}
		}
	}

	if err := encoder.Int64(math.MinInt64); assert.NoError(t, err) {
		if v, err := decoder.Int64(); assert.NoError(t, err) {
			assert.Equal(t, int64(math.MinInt64), v)
		}
	}

	if err := encoder.Int64(math.MaxInt64); assert.NoError(t, err) {
		if v, err := decoder.Int64(); assert.NoError(t, err) {
			assert.Equal(t, int64(math.MaxInt64), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Int64(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Int64(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, int64(0), v)
		}
	}
}

func Test_UInt8(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := 0; i <= 255; i++ {
		if err := encoder.UInt8(uint8(i)); assert.NoError(t, err) {
			if v, err := decoder.UInt8(); assert.NoError(t, err) {
				assert.Equal(t, uint8(i), v)
			}
		}
	}

	if err := encoder.UInt8(math.MaxUint8); assert.NoError(t, err) {
		if v, err := decoder.UInt8(); assert.NoError(t, err) {
			assert.Equal(t, uint8(math.MaxUint8), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).UInt8(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).UInt8(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, uint8(0), v)
		}
	}
}

func Test_UInt16(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := 0; i <= 65535; i += 10 {
		if err := encoder.UInt16(uint16(i)); assert.NoError(t, err) {
			if v, err := decoder.UInt16(); assert.NoError(t, err) {
				assert.Equal(t, uint16(i), v)
			}
		}
	}

	if err := encoder.UInt16(math.MaxUint16); assert.NoError(t, err) {
		if v, err := decoder.UInt16(); assert.NoError(t, err) {
			assert.Equal(t, uint16(math.MaxUint16), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).UInt16(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).UInt16(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, uint16(0), v)
		}
	}
}
func Test_UInt32(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := 0; i <= 4294967295; i += 100000 {
		if err := encoder.UInt32(uint32(i)); assert.NoError(t, err) {
			if v, err := decoder.UInt32(); assert.NoError(t, err) {
				assert.Equal(t, uint32(i), v)
			}
		}
	}

	if err := encoder.UInt32(math.MaxUint32); assert.NoError(t, err) {
		if v, err := decoder.UInt32(); assert.NoError(t, err) {
			assert.Equal(t, uint32(math.MaxUint32), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).UInt32(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).UInt32(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, uint32(0), v)
		}
	}
}

func Test_UInt64(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := 0; i <= 4294967295*2; i += 100000 {
		if err := encoder.UInt64(uint64(i)); assert.NoError(t, err) {
			if v, err := decoder.UInt64(); assert.NoError(t, err) {
				assert.Equal(t, uint64(i), v)
			}
		}
	}

	if err := encoder.UInt64(math.MaxUint64); assert.NoError(t, err) {
		if v, err := decoder.UInt64(); assert.NoError(t, err) {
			assert.Equal(t, uint64(math.MaxUint64), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).UInt64(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).UInt64(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, uint64(0), v)
		}
	}
}

func Test_Float32(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -2147483648; i <= 2147483647; i += 100000 {
		if err := encoder.Float32(float32(i)); assert.NoError(t, err) {
			if v, err := decoder.Float32(); assert.NoError(t, err) {
				assert.Equal(t, float32(i), v)
			}
		}
	}

	if err := encoder.Float32(math.MinInt32); assert.NoError(t, err) {
		if v, err := decoder.Float32(); assert.NoError(t, err) {
			assert.Equal(t, float32(math.MinInt32), v)
		}
	}

	if err := encoder.Float32(math.MaxInt32); assert.NoError(t, err) {
		if v, err := decoder.Float32(); assert.NoError(t, err) {
			assert.Equal(t, float32(math.MaxInt32), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Float32(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Float32(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, float32(0), v)
		}
	}
}

func Test_Float64(t *testing.T) {
	var (
		buf     bytes.Buffer
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	for i := -2147483648; i <= 2147483647*2; i += 100000 {
		if err := encoder.Float64(float64(i)); assert.NoError(t, err) {
			if v, err := decoder.Float64(); assert.NoError(t, err) {
				assert.Equal(t, float64(i), v)
			}
		}
	}

	if err := encoder.Float64(math.MinInt64); assert.NoError(t, err) {
		if v, err := decoder.Float64(); assert.NoError(t, err) {
			assert.Equal(t, float64(math.MinInt64), v)
		}
	}

	if err := encoder.Float64(math.MaxInt32); assert.NoError(t, err) {
		if v, err := decoder.Float64(); assert.NoError(t, err) {
			assert.Equal(t, float64(math.MaxInt32), v)
		}
	}

	if err := NewEncoder(&testErrorReadWriter{}).Float64(0); assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}

	if v, err := NewDecoder(&testErrorReadWriter{}).Float64(); assert.Error(t, err) {
		if assert.Equal(t, io.EOF, err) {
			assert.Equal(t, float64(0), v)
		}
	}
}

func Test_String(t *testing.T) {
	var (
		buf     bytes.Buffer
		str     = fmt.Sprintf("str_%d", time.Now().Unix())
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	if err := encoder.String(str); assert.NoError(t, err) {
		if v, err := decoder.String(); assert.NoError(t, err) {
			assert.Equal(t, str, v)
		}
	}
}

func Test_RawString(t *testing.T) {
	var (
		buf     bytes.Buffer
		str     = []byte(fmt.Sprintf("str_%d", time.Now().Unix()))
		encoder = NewEncoder(&buf)
		decoder = NewDecoder(&buf)
	)

	if err := encoder.RawString(str); assert.NoError(t, err) {
		if v, err := decoder.String(); assert.NoError(t, err) {
			assert.Equal(t, string(str), v)
		}
	}
}
