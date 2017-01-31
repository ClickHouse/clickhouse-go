package clickhouse

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_WriteBuffer(t *testing.T) {
	{
		if wb := wb(10); assert.Equal(t, int(0), wb.len()) {
			if assert.Len(t, wb.bytes(), 0) {
				var buf bytes.Buffer
				if err := wb.writeTo(&buf); assert.NoError(t, err) {
					assert.Len(t, buf.Bytes(), 0)
				}
			}
		}
	}
	{
		wb := wb(10)
		copy(wb.alloc(5), []byte{1, 2, 3, 4, 5})
		copy(wb.alloc(5), []byte{6, 7, 8, 9, 10})
		copy(wb.alloc(10), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
		if assert.Len(t, wb.chunks, 2) {
			if assert.Equal(t, int(20), wb.len()) {
				for n, chunck := range wb.chunks {
					t.Logf("chunk[%d]: %v", n, chunck)
				}
				wb.free()
				{
					if assert.Len(t, wb.chunks, 1) {
						if assert.Equal(t, int(0), wb.len()) {
							assert.Equal(t, int(10), cap(wb.chunks[0]))
						}
					}
				}
			}
		}
	}
}
