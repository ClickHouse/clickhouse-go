package binary

import (
	"encoding/binary"
	"io"
	"math"
)

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		output: w,
	}
}

type Encoder struct {
	output  io.Writer
	scratch [binary.MaxVarintLen64]byte
}

func (enc *Encoder) Raw(b []byte) error {
	if _, err := enc.output.Write(b); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Bool(v bool) error {
	if v {
		return enc.UInt8(1)
	}
	return enc.UInt8(0)
}

func (enc *Encoder) Byte(b byte) error {
	return enc.UInt8(b)
}

func (enc *Encoder) Int8(v int8) error {
	return enc.UInt8(uint8(v))
}

func (enc *Encoder) Int16(v int16) error {
	return enc.UInt16(uint16(v))
}

func (enc *Encoder) Int32(v int32) error {
	return enc.UInt32(uint32(v))
}

func (enc *Encoder) Int64(v int64) error {
	return enc.UInt64(uint64(v))
}

func (enc *Encoder) UInt8(v uint8) error {
	enc.scratch[0] = v
	if _, err := enc.output.Write(enc.scratch[:1]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt16(v uint16) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	if _, err := enc.output.Write(enc.scratch[:2]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt32(v uint32) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	if _, err := enc.output.Write(enc.scratch[:4]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt64(v uint64) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	enc.scratch[4] = byte(v >> 32)
	enc.scratch[5] = byte(v >> 40)
	enc.scratch[6] = byte(v >> 48)
	enc.scratch[7] = byte(v >> 56)
	if _, err := enc.output.Write(enc.scratch[:8]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Float32(v float32) error {
	return enc.UInt32(math.Float32bits(v))
}

func (enc *Encoder) Float64(v float64) error {
	return enc.UInt64(math.Float64bits(v))
}

func (enc *Encoder) Uvarint(v uint64) error {
	ln := binary.PutUvarint(enc.scratch[:binary.MaxVarintLen64], v)
	if _, err := enc.output.Write(enc.scratch[0:ln]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Flush() error {
	if w, ok := enc.output.(interface{ Flush() error }); ok {
		return w.Flush()
	}
	return nil
}

func (enc *Encoder) String(v string) error {
	str := str2Bytes(v)
	if err := enc.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := enc.output.Write(str); err != nil {
		return err
	}
	return nil
}
