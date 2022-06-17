package text

import (
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"
	"math/big"
	"net"
	"reflect"
	"time"
)

type Type string

func (t Type) ColumnType() (Interface, error) {
	switch t {
	case "Int64":
		return &intDecoder{bitSize: 64}, nil
	case "Int32":
		return &intDecoder{bitSize: 32}, nil
	case "Int16":
		return &intDecoder{bitSize: 16}, nil
	case "Int8":
		return &intDecoder{bitSize: 8}, nil
	case "UInt64":
		return &uintDecoder{bitSize: 64}, nil
	case "UInt32":
		return &uintDecoder{bitSize: 32}, nil
	case "UInt16":
		return &uintDecoder{bitSize: 16}, nil
	case "UInt8":
		return &uintDecoder{bitSize: 8}, nil
	case "Bool":
		return &uintDecoder{bitSize: 8}, nil
	case "String":
		return &stringDecoder{}, nil
	}

	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

var (
	scanTypeFloat32      = reflect.TypeOf(float32(0))
	scanTypeFloat64      = reflect.TypeOf(float64(0))
	scanTypeInt8         = reflect.TypeOf(int8(0))
	scanTypeInt16        = reflect.TypeOf(int16(0))
	scanTypeInt32        = reflect.TypeOf(int32(0))
	scanTypeInt64        = reflect.TypeOf(int64(0))
	scanTypeUInt8        = reflect.TypeOf(uint8(0))
	scanTypeUInt16       = reflect.TypeOf(uint16(0))
	scanTypeUInt32       = reflect.TypeOf(uint32(0))
	scanTypeUInt64       = reflect.TypeOf(uint64(0))
	scanTypeIP           = reflect.TypeOf(net.IP{})
	scanTypeBool         = reflect.TypeOf(true)
	scanTypeByte         = reflect.TypeOf([]byte{})
	scanTypeUUID         = reflect.TypeOf(uuid.UUID{})
	scanTypeTime         = reflect.TypeOf(time.Time{})
	scanTypeRing         = reflect.TypeOf(orb.Ring{})
	scanTypePoint        = reflect.TypeOf(orb.Point{})
	scanTypeSlice        = reflect.TypeOf([]interface{}{})
	scanTypeBigInt       = reflect.TypeOf(&big.Int{})
	scanTypeString       = reflect.TypeOf("")
	scanTypePolygon      = reflect.TypeOf(orb.Polygon{})
	scanTypeDecimal      = reflect.TypeOf(decimal.Decimal{})
	scanTypeMultiPolygon = reflect.TypeOf(orb.MultiPolygon{})
)
