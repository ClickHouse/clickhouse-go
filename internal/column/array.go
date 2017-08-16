package column

import (
	"fmt"
	"reflect"
	"time"

	"github.com/kshvakov/clickhouse/internal/binary"
	"github.com/kshvakov/clickhouse/internal/types"
)

type Array struct {
	base
	column Column
}

func (array *Array) Read(decoder *binary.Decoder) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Array(T) column")
}

func (array *Array) Write(encoder *binary.Encoder, v interface{}) error {
	return fmt.Errorf("do not use Write method for Array(T) column")
}

func (array *Array) ReadArray(decoder *binary.Decoder, ln int) (interface{}, error) {
	slice := reflect.MakeSlice(array.scanType, 0, ln)
	for i := 0; i < ln; i++ {
		value, err := array.column.Read(decoder)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(value))
	}
	return slice.Interface(), nil
}

func (array *Array) WriteArray(encoder *binary.Encoder, v interface{}) (uint64, error) {
	switch value := v.(type) {
	case *types.Array:
		_ = value
	}
	return 0, nil
}

func parseArray(name, chType string, timezone *time.Location) (*Array, error) {
	if len(chType) < 11 {
		return nil, fmt.Errorf("invalid Array column type: %s", chType)
	}
	column, err := Factory(name, chType[6:][:len(chType)-7], timezone)
	if err != nil {
		return nil, fmt.Errorf("array: %v", err)
	}

	var scanType interface{}
	switch t := column.ScanType().Kind(); t {
	case reflect.Int8:
		scanType = []int8{}
	case reflect.Int16:
		scanType = []int16{}
	case reflect.Int32:
		scanType = []int32{}
	case reflect.Int64:
		scanType = []int64{}
	case reflect.Uint8:
		scanType = []uint8{}
	case reflect.Uint16:
		scanType = []uint16{}
	case reflect.Uint32:
		scanType = []uint32{}
	case reflect.Uint64:
		scanType = []uint64{}
	case reflect.Float32:
		scanType = []float32{}
	case reflect.Float64:
		scanType = []float64{}
	case reflect.String:
		scanType = []string{}
	case scanTypes[time.Time{}].Kind():
		scanType = []time.Time{}
	default:
		return nil, fmt.Errorf("unsupported array type '%s'", column.ScanType().Name())
	}
	return &Array{
		base: base{
			name:     name,
			chType:   chType,
			scanType: reflect.TypeOf(scanType),
		},
		column: column,
	}, nil
}
