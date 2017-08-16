package data

import (
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
	"github.com/kshvakov/clickhouse/lib/types"
)

func (block *Block) WriteDate(c int, v time.Time) error {
	return block.Buffers[c].Column.UInt16(uint16(v.Unix() / 24 / 3600))
}

func (block *Block) WriteDateTime(c int, v time.Time) error {
	return block.Buffers[c].Column.UInt32(uint32(v.Unix()))
}

func (block *Block) WriteUInt8(c int, v uint8) error {
	return block.Buffers[c].Column.UInt8(v)
}

func (block *Block) WriteUInt16(c int, v uint16) error {
	return block.Buffers[c].Column.UInt16(v)
}

func (block *Block) WriteUInt32(c int, v uint32) error {
	return block.Buffers[c].Column.UInt32(v)
}

func (block *Block) WriteUInt64(c int, v uint64) error {
	return block.Buffers[c].Column.UInt64(v)
}

func (block *Block) WriteFloat32(c int, v float32) error {
	return block.Buffers[c].Column.Float32(v)
}

func (block *Block) WriteFloat64(c int, v float64) error {
	return block.Buffers[c].Column.Float64(v)
}

func (block *Block) WriteBytes(c int, v []byte) error {
	if err := block.Buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.Buffers[c].Column.Write(v); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteString(c int, v string) error {
	if err := block.Buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.Buffers[c].Column.Write(binary.Str2Bytes(v)); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteFixedString(c int, v []byte) error {
	return block.Columns[c].Write(block.Buffers[c].Column, v)
}

func (block *Block) WriteArray(c int, v *types.Array) error {
	// @todo: write array
	block.offsets[c] += 0
	return block.Buffers[c].Offset.UInt64(block.offsets[c])
}
