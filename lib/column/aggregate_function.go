package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"reflect"
	"strings"
	"time"
)

type AggregateFunction struct {
	base
	column   Column
	function string
}

func (agg *AggregateFunction) ScanType() reflect.Type {
	return agg.column.ScanType()
}

func (agg *AggregateFunction) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return agg.column.Read(decoder, isNull)
}

func (agg *AggregateFunction) Write(encoder *binary.Encoder, v interface{}) error {
	return agg.column.Write(encoder, v)
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
			name:   name,
			chType: chType,
		},
		column: column,
	}, nil
}
