package column

import (
	"errors"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Nothing struct{}

func (Nothing) Type() Type                     { return "Nothing" }
func (Nothing) ScanType() reflect.Type         { return reflect.TypeOf(nil) }
func (Nothing) Rows() int                      { return 0 }
func (Nothing) Row(int, bool) interface{}      { return nil }
func (Nothing) ScanRow(interface{}, int) error { return nil }
func (Nothing) Append(interface{}) ([]uint8, error) {
	return nil, &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}
func (Nothing) AppendRow(interface{}) error {
	return &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}
func (Nothing) Decode(decoder *binary.Decoder, rows int) error {
	scratch := make([]byte, rows)
	if err := decoder.Raw(scratch); err != nil {
		return err
	}
	return nil
}
func (Nothing) Encode(*binary.Encoder) error {
	return &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}

var _ Interface = (*Nothing)(nil)
