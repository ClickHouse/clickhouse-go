package column

import (
	"fmt"
	"reflect"
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
)

type Nullable struct {
	base
	column Column
}

func (null *Nullable) ScanType() reflect.Type {
	return null.column.ScanType()
}

func (null *Nullable) Read(decoder *binary.Decoder) (interface{}, error) {
	isNull, err := decoder.Uvarint()
	switch {
	case err != nil:
		return nil, err
	case isNull == 1:
		if _, err := null.column.Read(decoder); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return null.column.Read(decoder)
}

func (null *Nullable) Write(encoder *binary.Encoder, v interface{}) error {
	if v == nil {
		if err := encoder.Uvarint(1); err != nil {
			return err
		}
		return null.column.Write(encoder, null.column.defaultValue())
	}
	if err := encoder.Uvarint(0); err != nil {
		return err
	}
	return null.column.Write(encoder, v)
}

func parseNullable(name, chType string, timezone *time.Location) (*Nullable, error) {
	if len(chType) < 14 {
		return nil, fmt.Errorf("invalid Nullable column type: %s", chType)
	}
	column, err := Factory(name, chType[9:][:len(chType)-10], timezone)
	if err != nil {
		return nil, fmt.Errorf("Nullable(T): %v", err)
	}
	return &Nullable{
		base: base{
			name:   name,
			chType: chType,
		},
		column: column,
	}, nil
}
