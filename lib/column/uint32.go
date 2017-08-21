package column

import (
	"github.com/kshvakov/clickhouse/lib/binary"
)

type UInt32 struct{ base }

func (UInt32) Read(decoder *binary.Decoder) (interface{}, error) {
	v, err := decoder.UInt32()
	if err != nil {
		return uint32(0), err
	}
	return v, nil
}

func (UInt32) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case uint32:
		return encoder.UInt32(v)
	case int64:
		return encoder.UInt32(uint32(v))
	}
	return &ErrUnexpectedType{v}
}
