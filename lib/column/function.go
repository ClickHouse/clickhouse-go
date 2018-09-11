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

	return &AggregateFunction{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: reflect.ValueOf(column.ScanType()),
		},
	}, nil
}
