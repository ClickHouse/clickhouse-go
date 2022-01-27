package binary

import (
	"reflect"
	"unsafe"
)

func unsafeStr2Bytes(str string) []byte {
	// Copied from https://github.com/m3db/m3/blob/master/src/x/unsafe/string.go#L62
	if len(str) == 0 {
		return nil
	}
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&str)).Data

	l := len(str)
	byteHeader.Len = l
	byteHeader.Cap = l

	return b
}
