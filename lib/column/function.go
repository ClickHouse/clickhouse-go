package column

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
)

type AggregateFunction struct {
	base
	function string
	column Column
}

func (array *AggregateFunction) Read(decoder *binary.Decoder) (interface{}, error) {
	return nil, fmt.Errorf("aggregate functions cannot be directly read")
}

func (array *AggregateFunction) Write(encoder *binary.Encoder, v interface{}) error {
	return fmt.Errorf("aggregate functions cannot be directly written")
}

func parseAggregateFunction(name, chType string, timezone *time.Location) (*AggregateFunction, error) {
	parensContents := strings.Split(chType[18:len(chType)-1], ",")
	if len(parensContents) != 2 {
		return nil, fmt.Errorf("AggregateFunction: %v", "not enough arguments")
	}

	column, err := Factory(name, strings.TrimSpace(parensContents[1]), timezone)
	if err != nil {
		return nil, fmt.Errorf("AggregateFunction: %v", err)
	}

	var scanTypes = map[reflect.Kind]reflect.Value{
		reflect.Int8:     reflect.ValueOf(int8(0)),
		reflect.Int16:    reflect.ValueOf(int16(0)),
		reflect.Int32:    reflect.ValueOf(int32(0)),
		reflect.Int64:    reflect.ValueOf(int64(0)),
		reflect.Uint8:    reflect.ValueOf(uint8(0)),
		reflect.Uint16:   reflect.ValueOf(uint16(0)),
		reflect.Uint32:   reflect.ValueOf(uint32(0)),
		reflect.Uint64:   reflect.ValueOf(uint64(0)),
		reflect.Float32:  reflect.ValueOf(float32(0)),
		reflect.Float64:  reflect.ValueOf(float64(0)),
		reflect.String:   reflect.ValueOf(string("")),
		// not sure what to do about time.Time
	}

	return &AggregateFunction{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: scanTypes[column.ScanType().Kind()],
		},
		function: strings.TrimSpace(parensContents[0]),
		column: column,
	}, nil
}
