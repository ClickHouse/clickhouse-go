package compress

import (
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/cityhash102"
	"github.com/pierrec/lz4/v4"
)

func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		wr:    wr,
		data:  make([]byte, maxBlockSize),
		zdata: make([]byte, lz4.CompressBlockBound(maxBlockSize)+headerSize),
	}
}

type Writer struct {
	wr         io.Writer
	pos        int
	data       []byte
	zdata      []byte
	compressor lz4.Compressor
}

func (w *Writer) Write(p []byte) (n int, err error) {
	for len(p) > 0 {
		m := copy(w.data[w.pos:], p)
		w.pos += m
		p = p[m:]
		if w.pos == len(w.data) {
			if err = w.Flush(); err != nil {
				return n, err
			}
		}
		n += m
	}
	return n, nil
}

func (w *Writer) Flush() (err error) {
	if w.pos == 0 {
		return
	}
	compressedSize, err := w.compressor.CompressBlock(w.data[:w.pos], w.zdata[headerSize:])
	if err != nil {
		return err
	}
	compressedSize += compressHeaderSize
	// fill the header, compressed_size_32 + uncompressed_size_32
	w.zdata[16] = LZ4
	endian.PutUint32(w.zdata[17:], uint32(compressedSize))
	endian.PutUint32(w.zdata[21:], uint32(w.pos))
	// fill the checksum
	checkSum := cityhash102.CityHash128(w.zdata[16:], uint32(compressedSize))
	{
		endian.PutUint64(w.zdata[0:], checkSum.Lower64())
		endian.PutUint64(w.zdata[8:], checkSum.Higher64())
	}
	if _, err := w.wr.Write(w.zdata[:compressedSize+checksumSize]); err != nil {
		return err
	}
	/*if w, ok := cw.writer.(WriteFlusher); ok {
		err = w.Flush()
	}*/
	w.pos = 0
	return
}

func (w *Writer) Close() error {
	w.data = nil
	w.zdata = nil
	return nil
}
