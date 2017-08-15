package column

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type Column interface {
	Name() string
	CHType() string
	ScanType() reflect.Type
	Read(*binary.Decoder) (interface{}, error)
	Write(*binary.Encoder, interface{}) error
}

func Factory(name, chType string, timezone *time.Location) (Column, error) {
	switch chType {
	case "Int8":
		return &Int8{
			base: base{
				name:     name,
				chType:   chType,
				scanType: reflect.TypeOf(int8(0)),
			},
		}, nil
	case "Int16":
		return &Int16{
			base: base{
				name:     name,
				chType:   chType,
				scanType: reflect.TypeOf(int16(0)),
			},
		}, nil
	case "String":
		return &String{
			base: base{
				name:     name,
				chType:   chType,
				scanType: reflect.TypeOf(string("")),
			},
		}, nil
	case "DateTime":
	}

	switch {
	case strings.HasPrefix(chType, "FixedString"):
		var strLen int
		if _, err := fmt.Sscanf(chType, "FixedString(%d)", &strLen); err != nil {
			return nil, err
		}
		return &FixedString{
			base: base{
				name:     name,
				chType:   chType,
				scanType: reflect.TypeOf(string("")),
			},
			len:      strLen,
		}, nil
	case strings.HasPrefix(chType, "Enum8"), strings.HasPrefix(chType, "Enum16"):
		return parseEnum(name, chType)
	case strings.HasPrefix(chType, "Array"):
	}
	return nil, fmt.Errorf("column: unhandled type %v", chType)
}

type base struct {
	name, chType string
	scanType     reflect.Type
}

func (base *base) Name() string {
	return base.name
}

func (base *base) CHType() string {
	return base.chType
}

func (base *base) ScanType() reflect.Type {
	return base.scanType
}

func (base *base) String() string {
	return fmt.Sprintf("%s (%s)", base.name, base.chType)
}
