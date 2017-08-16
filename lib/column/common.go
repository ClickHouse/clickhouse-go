package column

import (
	"fmt"
	"reflect"
	"time"
)

var scanTypes = map[interface{}]reflect.Type{
	int8(0):     reflect.TypeOf(int8(0)),
	int16(0):    reflect.TypeOf(int16(0)),
	int32(0):    reflect.TypeOf(int32(0)),
	int64(0):    reflect.TypeOf(int64(0)),
	uint8(0):    reflect.TypeOf(uint8(0)),
	uint16(0):   reflect.TypeOf(uint16(0)),
	uint32(0):   reflect.TypeOf(uint32(0)),
	uint64(0):   reflect.TypeOf(uint64(0)),
	float32(0):  reflect.TypeOf(float32(0)),
	float64(0):  reflect.TypeOf(float64(0)),
	string(""):  reflect.TypeOf(string("")),
	time.Time{}: reflect.TypeOf(time.Time{}),
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
