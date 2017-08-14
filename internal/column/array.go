package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Array struct {
	name, chType string
	base         Column
	sliceType    interface{}
}

func (array *Array) Name() string {
	return array.name
}

func (array *Array) CHType() string {
	return array.chType
}

func (array *Array) Read(decoder *binary.Decoder) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Array(T) column")
}

func (array *Array) ReadArray(decoder *binary.Decoder, ln int) (interface{}, error) {

	return nil, nil
}

func (array *Array) Write(encoder *binary.Encoder, v interface{}) error {
	return nil
}

func (array *Array) String() string {
	return fmt.Sprintf("%s (%s)", array.name, array.chType)
}
