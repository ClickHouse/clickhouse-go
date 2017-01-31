package clickhouse

import (
	"io"
)

func wb(size int) *writeBuffer {
	return &writeBuffer{
		chunks: [][]byte{
			make([]byte, 0, size),
		},
	}
}

type writeBuffer struct {
	chunks [][]byte
}

func (wb *writeBuffer) Write(b []byte) (int, error) {
	return 0, nil
}

func (wb *writeBuffer) alloc(size int) []byte {
	var (
		chunkIdx = len(wb.chunks) - 1
		curChunk = wb.chunks[chunkIdx]
		chunkLen = len(curChunk)
		freeSize = cap(curChunk) - chunkLen
		max      = func(a, b int) int {
			if b > a {
				return b
			}
			return a
		}
	)
	if freeSize < size {
		wb.chunks = append(wb.chunks, make([]byte, size, max(size, cap(wb.chunks[0]))))
		return wb.chunks[chunkIdx+1]
	}
	wb.chunks[chunkIdx] = curChunk[:chunkLen+size]
	return wb.chunks[chunkIdx][chunkLen : chunkLen+size]
}

func (wb *writeBuffer) writeTo(w io.Writer) error {
	defer wb.free()
	for _, chunk := range wb.chunks {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (wb *writeBuffer) bytes() []byte {
	if len(wb.chunks) == 1 {
		return wb.chunks[0]
	}
	bytes := make([]byte, 0, wb.len())
	for _, chunk := range wb.chunks {
		bytes = append(bytes, chunk...)
	}
	return bytes
}

func (wb *writeBuffer) len() int {
	var v int
	for _, chunk := range wb.chunks {
		v += len(chunk)
	}
	return v
}
func (wb *writeBuffer) free() {
	wb.chunks = [][]byte{
		wb.chunks[0][0:0],
	}
}
