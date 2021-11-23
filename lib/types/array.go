package types

import (
	"database/sql/driver"
)

// some orm wrong with Array(T)
// add arrays driver valuer

type ArrayInt []int

func (aInt ArrayInt) Value() (driver.Value, error) {
	return aInt, nil
}

type ArrayInt64 []int64

func (aInt64 ArrayInt64) Value() (driver.Value, error) {
	return aInt64, nil
}

type ArrayUInt []uint

func (aUInt ArrayUInt) Value() (driver.Value, error) {
	return aUInt, nil
}

type ArrayUInt64 []uint64

func (aUInt ArrayUInt64) Value() (driver.Value, error) {
	return aUInt, nil
}

type ArrayString []string

func (aString ArrayString) Value() (driver.Value, error) {
	return aString, nil
}

var (
	_ driver.Valuer = ArrayInt{}
	_ driver.Valuer = ArrayInt64{}
	_ driver.Valuer = ArrayUInt{}
	_ driver.Valuer = ArrayUInt64{}
	_ driver.Valuer = ArrayString{}
)
