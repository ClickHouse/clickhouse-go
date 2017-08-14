package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Int8 struct {
	name, chType string
}

func (i *Int8) Name() string {
	return i.name
}

func (i *Int8) CHType() string {
	return i.chType
}

func (i *Int8) Read(decoder *binary.Decoder) (interface{}, error) {
	v, err := decoder.Int8()
	if err != nil {
		return int8(0), err
	}
	return v, nil
}

func (i *Int8) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case int8:
		return encoder.Int8(v)
	case int64:
		return encoder.Int8(int8(v))
	}
	return fmt.Errorf("unexpected type %T", v)
}

func (i *Int8) String() string {
	return fmt.Sprintf("%s (%s)", i.name, i.chType)
}
