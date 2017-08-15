package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Array struct {
	base
	baseColumn Column
	sliceType  interface{}
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
