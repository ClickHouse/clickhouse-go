package text

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
)

//case "Float32":
//case "Float64":
//case "Int8":
//case "Int16":
//case "Int32":
//case "Int64":
//case "UInt8":
//case "UInt16":
//case "UInt32":
//case "UInt64":
//case "Int128":
//case "UInt128":
//case "Int256":
//case "UInt256":
//case "IPv4":
//case "IPv6":
//case "Bool", "Boolean":
//case "Date":
//case "Date32":
//case "UUID":
//case "Nothing":
//case "Ring":
//case "Polygon":
//case "MultiPolygon":
//case "Point":
//case "String":

type Interface interface {
	Decode(val string) (driver.Value, error)
	Type() reflect.Type
}

type textDecoder struct{}

func (d *textDecoder) Decode(val string, columnDecoder Interface) (driver.Value, error) {
	value, err := columnDecoder.Decode(val)
	if err != nil {
		return nil, err
	}
	return value, nil
}

type stringDecoder struct{}

func (d *stringDecoder) Decode(val string) (driver.Value, error) {
	return val, nil
}

func (d *stringDecoder) Type() reflect.Type {
	return scanTypeString
}

type intDecoder struct {
	bitSize int
}

func (d *intDecoder) Decode(val string) (driver.Value, error) {
	v, err := strconv.ParseInt(val, 10, d.bitSize)
	switch d.bitSize {
	case 8:
		return int8(v), err
	case 16:
		return int16(v), err
	case 32:
		return int32(v), err
	case 64:
		return v, err
	default:
		return nil, fmt.Errorf("unsupported bit size %v", err)
	}
}

func (d *intDecoder) Type() reflect.Type {
	switch d.bitSize {
	case 8:
		return scanTypeInt8
	case 16:
		return scanTypeInt16
	case 32:
		return scanTypeInt32
	case 64:
		return scanTypeInt64
	default:
		panic("unsupported reflect type")
	}
}

type uintDecoder struct {
	bitSize int
}

func (d *uintDecoder) Decode(val string) (driver.Value, error) {
	v, err := strconv.ParseUint(val, 10, d.bitSize)
	switch d.bitSize {
	case 8:
		return uint8(v), err
	case 16:
		return uint16(v), err
	case 32:
		return uint32(v), err
	case 64:
		return v, err
	default:
		return nil, fmt.Errorf("unsupported bit size %v", err)
	}
}

func (d *uintDecoder) Type() reflect.Type {
	switch d.bitSize {
	case 8:
		return scanTypeUInt8
	case 16:
		return scanTypeUInt16
	case 32:
		return scanTypeUInt32
	case 64:
		return scanTypeUInt64
	default:
		panic("unsupported reflect type")
	}
}

type boolDecoder struct{}

func (d *boolDecoder) Decode(val string) (driver.Value, error) {
	if val == "true" {
		return true, nil
	}
	return false, nil
}

func (d *boolDecoder) Type() reflect.Type {
	return scanTypeBool
}
