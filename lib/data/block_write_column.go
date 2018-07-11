package data

import (
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
	"github.com/kshvakov/clickhouse/lib/column"
	"github.com/kshvakov/clickhouse/lib/types"
)

func (block *Block) WriteDate(c int, v time.Time) error {
	return block.buffers[c].Column.UInt16(uint16(v.Unix() / 24 / 3600))
}

func (block *Block) WriteDateNullable(c int, v time.Time) error {
	return block.buffers[c].Column.UInt16Nullable(uint16(v.Unix() / 24 / 3600))
}

func (block *Block) WriteDateTime(c int, v time.Time) error {
	return block.buffers[c].Column.UInt32(uint32(v.Unix()))
}

func (block *Block) WriteDateTimeNullable(c int, v time.Time) error {
	return block.buffers[c].Column.UInt32Nullable(uint32(v.Unix()))
}

func (block *Block) WriteInt8(c int, v int8) error {
	return block.buffers[c].Column.Int8(v)
}

func (block *Block) WriteInt8Nullable(c int, v int8) error {
	return block.buffers[c].Column.Int8Nullable(v)
}

func (block *Block) WriteInt16(c int, v int16) error {
	return block.buffers[c].Column.Int16(v)
}

func (block *Block) WriteInt16Nullable(c int, v int16) error {
	return block.buffers[c].Column.Int16Nullable(v)
}

func (block *Block) WriteInt32(c int, v int32) error {
	return block.buffers[c].Column.Int32(v)
}

func (block *Block) WriteInt32Nullable(c int, v int32) error {
	return block.buffers[c].Column.Int32Nullable(v)
}

func (block *Block) WriteInt64(c int, v int64) error {
	return block.buffers[c].Column.Int64(v)
}

func (block *Block) WriteInt64Nullable(c int, v int64) error {
	return block.buffers[c].Column.Int64Nullable(v)
}

func (block *Block) WriteUInt8(c int, v uint8) error {
	return block.buffers[c].Column.UInt8(v)
}

func (block *Block) WriteUInt8Nullable(c int, v uint8) error {
	return block.buffers[c].Column.UInt8Nullable(v)
}

func (block *Block) WriteUInt16(c int, v uint16) error {
	return block.buffers[c].Column.UInt16(v)
}

func (block *Block) WriteUInt16Nullable(c int, v uint16) error {
	return block.buffers[c].Column.UInt16Nullable(v)
}

func (block *Block) WriteUInt32(c int, v uint32) error {
	return block.buffers[c].Column.UInt32(v)
}

func (block *Block) WriteUInt32Nullable(c int, v uint32) error {
	return block.buffers[c].Column.UInt32Nullable(v)
}

func (block *Block) WriteUInt64(c int, v uint64) error {
	return block.buffers[c].Column.UInt64(v)
}

func (block *Block) WriteUInt64Nullable(c int, v uint64) error {
	return block.buffers[c].Column.UInt64Nullable(v)
}

func (block *Block) WriteFloat32(c int, v float32) error {
	return block.buffers[c].Column.Float32(v)
}

func (block *Block) WriteFloat32Nullable(c int, v float32) error {
	return block.buffers[c].Column.Float32Nullable(v)
}

func (block *Block) WriteFloat64(c int, v float64) error {
	return block.buffers[c].Column.Float64(v)
}

func (block *Block) WriteFloat64Nullable(c int, v float64) error {
	return block.buffers[c].Column.Float64Nullable(v)
}

func (block *Block) WriteBytes(c int, v []byte) error {
	if err := block.buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.buffers[c].Column.Write(v); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteBytesNullable(c int, v []byte) error {
	if err := block.buffers[c].Column.Nullable(); err != nil {
		return err
	}
	return block.WriteBytes(c, v)
}

func (block *Block) WriteString(c int, v string) error {
	if err := block.buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.buffers[c].Column.Write(binary.Str2Bytes(v)); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteStringNullable(c int, v string) error {
	if err := block.buffers[c].Column.Nullable(); err != nil {
		return err
	}
	return block.WriteString(c, v)
}

func (block *Block) WriteFixedString(c int, v []byte) error {
	return block.Columns[c].Write(block.buffers[c].Column, v)
}

func (block *Block) WriteFixedStringNullable(c int, v []byte) error {
	if err := block.buffers[c].Column.Nullable(); err != nil {
		return err
	}
	return block.Columns[c].Write(block.buffers[c].Column, v)
}

func (block *Block) WriteArray(c int, v *types.Array) error {
	ln, err := block.Columns[c].(*column.Array).WriteArray(block.buffers[c].Column, v)
	if err != nil {
		return err
	}
	block.offsets[c] += ln
	return block.buffers[c].Offset.UInt64(block.offsets[c])
}

func (block *Block) WriteArrayNullable(c int, v *types.Array) error {
	if err := block.buffers[c].Column.Nullable(); err != nil {
		return err
	}
	return block.WriteArray(c, v)
}
