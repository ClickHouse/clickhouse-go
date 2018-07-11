package binary

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"unsafe"
)

func NewEncoder(output io.Writer) *Encoder {
	return &Encoder{
		output: output,
	}
}

type Encoder struct {
	output  io.Writer
	scratch [binary.MaxVarintLen64]byte
}

func (enc *Encoder) Nullable() error {
	if _, err := enc.output.Write([]byte{0}); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Uvarint(v uint64) error {
	ln := binary.PutUvarint(enc.scratch[:binary.MaxVarintLen64], v)
	if _, err := enc.output.Write(enc.scratch[0:ln]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UvarintNullable(v uint64) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Uvarint(v)
}

func (enc *Encoder) Bool(v bool) error {
	if v {
		return enc.UInt8(1)
	}
	return enc.UInt8(0)
}

func (enc *Encoder) BoolNullable(v bool) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Bool(v)
}

func (enc *Encoder) Int8(v int8) error {
	return enc.UInt8(uint8(v))
}

func (enc *Encoder) Int8Nullable(v int8) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Int8(v)
}

func (enc *Encoder) Int16(v int16) error {
	return enc.UInt16(uint16(v))
}

func (enc *Encoder) Int16Nullable(v int16) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Int16(v)
}

func (enc *Encoder) Int32(v int32) error {
	return enc.UInt32(uint32(v))
}

func (enc *Encoder) Int32Nullable(v int32) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Int32(v)
}

func (enc *Encoder) Int64(v int64) error {
	return enc.UInt64(uint64(v))
}

func (enc *Encoder) Int64Nullable(v int64) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Int64(v)
}

func (enc *Encoder) UInt8(v uint8) error {
	enc.scratch[0] = v
	if _, err := enc.output.Write(enc.scratch[:1]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt8Nullable(v uint8) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.UInt8(v)
}

func (enc *Encoder) UInt16(v uint16) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	if _, err := enc.output.Write(enc.scratch[:2]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt16Nullable(v uint16) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.UInt16(v)
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

func (enc *Encoder) UInt32Nullable(v uint32) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.UInt32(v)
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

func (enc *Encoder) UInt64Nullable(v uint64) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.UInt64(v)
}

func (enc *Encoder) Float32(v float32) error {
	return enc.UInt32(math.Float32bits(v))
}

func (enc *Encoder) Float32Nullable(v float32) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Float32(v)
}

func (enc *Encoder) Float64(v float64) error {
	return enc.UInt64(math.Float64bits(v))
}

func (enc *Encoder) Float64Nullable(v float64) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.Float64(v)
}

func (enc *Encoder) String(v string) error {
	str := Str2Bytes(v)
	if err := enc.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := enc.output.Write(str); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) StringNullable(v string) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.String(v)
}

func (enc *Encoder) RawString(str []byte) error {
	if err := enc.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := enc.output.Write(str); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) RawStringNullable(str []byte) error {
	if err := enc.Nullable(); err != nil {
		return err
	}
	return enc.RawString(str)
}

func (enc *Encoder) Write(b []byte) (int, error) {
	return enc.output.Write(b)
}

func Str2Bytes(str string) []byte {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&str))
	header.Len = len(str)
	header.Cap = header.Len
	return *(*[]byte)(unsafe.Pointer(header))
}
