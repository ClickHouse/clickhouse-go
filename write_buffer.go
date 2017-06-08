package clickhouse

import "io"
import "sync"

const WriteBufferInitialSize = 256 * 1024

// Recycle column buffers, preallocate column buffers
var chunkPool = sync.Pool{}

func wb(initSize int) *writeBuffer {
	wb := &writeBuffer{}
	wb.addChunk(0, initSize)
	return wb
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
		wb.addChunk(0, wb.calcCap(len(data)))
		chunkIdx++
	}
}

func (wb *writeBuffer) alloc(size int) []byte {
	var (
		chunkIdx = len(wb.chunks) - 1
		chunkLen = len(wb.chunks[chunkIdx])
	)
	if (cap(wb.chunks[chunkIdx]) - chunkLen) < size {
		wb.addChunk(size, wb.calcCap(size))
		return wb.chunks[chunkIdx+1]
	}
	wb.chunks[chunkIdx] = wb.chunks[chunkIdx][:chunkLen+size]
	return wb.chunks[chunkIdx][chunkLen : chunkLen+size]
}

func (wb *writeBuffer) addChunk(size, capacity int) {
	var chunk []byte
	if c, ok := chunkPool.Get().([]byte); ok && cap(c) >= size {
		chunk = c[:size]
	} else {
		chunk = make([]byte, size, capacity)
	}
	wb.chunks = append(wb.chunks, chunk)
}

func (wb *writeBuffer) writeTo(w io.Writer) error {
	for _, chunk := range wb.chunks {
		if _, err := w.Write(chunk); err != nil {
			wb.free()
			return err
		}
	}
	wb.free()
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
	dataSize = max(dataSize, 64)
	if len(wb.chunks) == 0 {
		return dataSize
	}
	// Always double the size of the last chunk
	return max(dataSize, cap(wb.chunks[len(wb.chunks)-1])*2)
}

func (wb *writeBuffer) free() {
	if len(wb.chunks) == 0 {
		return
	}
	// Recycle all chunks except the last one
	chunkSizeThreshold := cap(wb.chunks[0])
	for _, chunk := range wb.chunks[:len(wb.chunks)-1] {
		// Drain chunks smaller than the initial size
		if cap(chunk) >= chunkSizeThreshold {
			chunkPool.Put(chunk[:0])
		} else {
			chunkSizeThreshold = cap(chunk)
		}
	}
	// Keep the largest chunk
	wb.chunks[0] = wb.chunks[len(wb.chunks)-1][:0]
	wb.chunks = wb.chunks[:1]
}

func max(a, b int) int {
	if b > a {
		return b
	}
	return a
}
