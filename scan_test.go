package clickhouse

import (
	"fmt"
	"reflect"
	"testing"
)

func TestScanX(t *testing.T) {
	type T struct {
		Field  string
		Field2 string `db:"ccc"`
		Field3 string `clickhouse:"field"`
	}
	v := T{}

	base := reflect.TypeOf(v)
	if base.Kind() == reflect.Ptr {
		base = base.Elem()
	}

	nt := reflect.New(base)

	fmt.Println(nt)
}
