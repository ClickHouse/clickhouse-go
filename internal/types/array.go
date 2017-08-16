package types

import (
	"database/sql/driver"
)

type Array struct {
}

func (array *Array) Value() (driver.Value, error) {
	return nil, nil
}
