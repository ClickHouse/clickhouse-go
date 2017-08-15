package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Int16 struct{ base }

func (i *Int16) Read(decoder *binary.Decoder) (interface{}, error) {
	v, err := decoder.Int16()
	if err != nil {
		return int16(0), err
	}
	return v, nil
}

func (i *Int16) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case int16:
		return encoder.Int16(v)
	case int64:
		return encoder.Int16(int16(v))
	}
	return fmt.Errorf("unexpected type %T", v)
}
