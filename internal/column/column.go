package column

import (
	"fmt"
	"time"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Column interface {
	Name() string
	CHType() string
	Read(*binary.Decoder) (interface{}, error)
	Write(*binary.Encoder, interface{}) error
}

func Factory(name, chType string, timezone *time.Location) (Column, error) {
	switch chType {
	case "Int8":
		return &Int8{
			name:   name,
			chType: chType,
		}, nil
	case "String":
		return &String{
			name:   name,
			chType: chType,
		}, nil
	case "DateTime":
	}
	return nil, fmt.Errorf("column: unhandled type %v", chType)
}
