package clickhouse

// @todo: restore block_size functionality

import (
	"time"

	"github.com/kshvakov/clickhouse/lib/types"
)

// Statement supporting columnar writer
type ColumnarStatement interface {
	ColumnWriter() ColumnWriter
	ColumnWriterEnd(rows uint64) error
}

func (stmt *stmt) ColumnWriter() ColumnWriter {
	stmt.ch.block.Reserve()
	return stmt.ch.block
}

func (stmt *stmt) ColumnWriterEnd(rows uint64) error {
	stmt.ch.block.NumRows += rows
	return nil
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
	WriteArray(c int, v *types.Array) error
	WriteString(c int, v string) error
	WriteFixedString(c int, v []byte) error
}
