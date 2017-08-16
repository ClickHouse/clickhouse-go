package binary

import (
	//"bytes"
	"testing"
)

type tFixedReader struct {
	buf []byte
}

func (t *tFixedReader) Read(v []byte) (int, error) {
	copy(v, t.buf)
	return len(t.buf), nil
}

func Benchmark_T(b *testing.B) {
	bb := make([]byte, 6)
	b.Log(bb[:1])
	var decoder Decoder
	decoder.input = &tFixedReader{bb}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decoder.UInt64()
	}
}
