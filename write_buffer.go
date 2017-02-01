package clickhouse

import "io"

func wb(cap int) *writeBuffer {
	return &writeBuffer{
		chunks: [][]byte{
			make([]byte, 0, cap),
		},
	}
}

type writeBuffer struct{ chunks [][]byte }

func (wb *writeBuffer) Write(data []byte) (int, error) {
	var (
		chunkIdx = len(wb.chunks) - 1
		dataSize = len(data)
	)
	for {
		freeSize := cap(wb.chunks[chunkIdx]) - len(wb.chunks[chunkIdx])
		if freeSize >= len(data) {
			wb.chunks[chunkIdx] = append(wb.chunks[chunkIdx], data...)
			return dataSize, nil
		}
		wb.chunks[chunkIdx] = append(wb.chunks[chunkIdx], data[:freeSize]...)
		data = data[freeSize:]
		wb.chunks = append(wb.chunks, make([]byte, 0, wb.calcCap(dataSize)))
		chunkIdx++
	}
}

func (wb *writeBuffer) alloc(size int) []byte {
	var (
		chunkIdx = len(wb.chunks) - 1
		chunkLen = len(wb.chunks[chunkIdx])
	)
	if (cap(wb.chunks[chunkIdx]) - chunkLen) < size {
		wb.chunks = append(wb.chunks, make([]byte, size, wb.calcCap(size)))
		return wb.chunks[chunkIdx+1]
	}
	wb.chunks[chunkIdx] = wb.chunks[chunkIdx][:chunkLen+size]
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

func (wb *writeBuffer) calcCap(dataSize int) int {
	return max(dataSize, (wb.len()/len(wb.chunks))*2)
}

func (wb *writeBuffer) free() {
	wb.chunks = [][]byte{
		wb.chunks[0][0:0],
	}
}

func max(a, b int) int {
	if b > a {
		return b
	}
	return a
}
