package types

import (
	"database/sql/driver"
	"fmt"
)

type Bool bool

// Value implements the driver.Valuer interface.
func (u Bool) Value() (driver.Value, error) {
	var i8 int8 = 0
	if u {
		i8 = 1
	}

	return i8, nil
}

// Scan implements the sql.Scanner interface.
func (u *Bool) Scan(src interface{}) error {
	switch src := src.(type) {
	case int8:
		b := Bool(false)
		if src == 1 {
			b = Bool(true)
		}

		*u = b

		return nil
	}

	return fmt.Errorf("failed to convert %T to Bool", src)
}

type NullBool struct {
	Bool  Bool
	Valid bool
}

// Value implements the driver.Valuer interface.
func (u NullBool) Value() (driver.Value, error) {
	if !u.Valid {
		return nil, nil
	}
	return u.Bool.Value()
}

// Scan implements the sql.Scanner interface.
func (u *NullBool) Scan(src interface{}) error {
	if src == nil {
		u.Bool, u.Valid = false, false
		return nil
	}

	u.Valid = true
	return u.Bool.Scan(src)
}
