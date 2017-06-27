package clickhouse

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Statement supporting columnar writer
type ColumnarStatement interface {
	ColumnWriter() ColumnWriter
	ColumnWriterEnd(rows uint64) error
}

func (stmt *stmt) ColumnWriter() ColumnWriter {
	stmt.ch.data.reserveColumns()
	return stmt.ch.data
}

func (stmt *stmt) ColumnWriterEnd(rows uint64) error {
	writtenBlocks := stmt.counter / stmt.ch.blockSize
	stmt.counter += int(rows)
	stmt.ch.data.numRows += rows

	var err error
	if stmt.counter/stmt.ch.blockSize > writtenBlocks {
		err = stmt.ch.data.write(stmt.ch.serverRevision, stmt.ch.conn)
	}
	return err
}

// Interface for block writer allowing writes to individual columns
type ColumnWriter interface {
	WriteDate(c int, v time.Time) error
	WriteDateTime(c int, v time.Time) error
	WriteUInt8(c int, v uint8) error
	WriteUInt16(c int, v uint16) error
	WriteUInt32(c int, v uint32) error
	WriteUInt64(c int, v uint64) error
	WriteFloat32(c int, v float32) error
	WriteFloat64(c int, v float64) error
	WriteBytes(c int, v []byte) error
	WriteArray(c int, v *array) error
	WriteString(c int, v string) error
	WriteFixedString(c int, v []byte) error
}

func (b *block) WriteDate(c int, v time.Time) error {
	binary.LittleEndian.PutUint16(b.buffers[c].alloc(2), uint16(v.Unix()/24/3600))
	return nil
}

func (b *block) WriteDateTime(c int, v time.Time) error {
	binary.LittleEndian.PutUint32(b.buffers[c].alloc(4), uint32(v.Unix()))
	return nil
}

func (b *block) WriteUInt8(c int, v uint8) error {
	buf := b.buffers[c].alloc(1)
	buf[0] = v
	return nil
}

func (b *block) WriteUInt16(c int, v uint16) error {
	binary.LittleEndian.PutUint16(b.buffers[c].alloc(2), v)
	return nil
}

func (b *block) WriteUInt32(c int, v uint32) error {
	binary.LittleEndian.PutUint32(b.buffers[c].alloc(4), v)
	return nil
}

func (b *block) WriteUInt64(c int, v uint64) error {
	binary.LittleEndian.PutUint64(b.buffers[c].alloc(8), v)
	return nil
}

func (b *block) WriteFloat32(c int, v float32) error {
	binary.LittleEndian.PutUint32(b.buffers[c].alloc(4), math.Float32bits(v))
	return nil
}

func (b *block) WriteFloat64(c int, v float64) error {
	binary.LittleEndian.PutUint64(b.buffers[c].alloc(8), math.Float64bits(v))
	return nil
}

func (b *block) WriteBytes(c int, v []byte) error {
	var (
		scratch = make([]byte, binary.MaxVarintLen64)
		vlen    = binary.PutUvarint(scratch, uint64(len(v)))
	)
	if _, err := b.buffers[c].Write(scratch[:vlen]); err != nil {
		return err
	}
	if _, err := b.buffers[c].Write(v); err != nil {
		return err
	}
	return nil
}

func (b *block) WriteString(c int, v string) error {
	var (
		scratch = make([]byte, binary.MaxVarintLen64)
		vlen    = binary.PutUvarint(scratch, uint64(len(v)))
	)
	if _, err := b.buffers[c].Write(scratch[:vlen]); err != nil {
		return err
	}
	if _, err := b.buffers[c].Write([]byte(v)); err != nil {
		return err
	}
	return nil
}

func (b *block) WriteFixedString(c int, v []byte) error {
	strlen := len(b.columnInfo[c].([]byte))
	if len(v) > strlen {
		return fmt.Errorf("too large value")
	} else if len(v) == 0 {
		// When empty, insert default value to avoid allocation
		v = b.columnInfo[c].([]byte)
	} else if len(v) < strlen {
		fixedString := make([]byte, strlen)
		copy(fixedString, v)
		v = fixedString
	}
	if _, err := b.buffers[c].Write(v); err != nil {
		return err
	}
	return nil
}

func (b *block) WriteArray(c int, v *array) error {
	arrayLen, err := v.write(b.columnInfo[c], b.buffers[c])
	if err != nil {
		return err
	}
	b.offsets[c] += arrayLen
	if err := writeUInt64(b.offsetBuffers[c], b.offsets[c]); err != nil {
		return err
	}
	return nil
}
