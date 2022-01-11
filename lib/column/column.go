package column

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type null struct{}
type Type string

func (t Type) String() string {
	return string(t)
}

func (t Type) Base() Type {
	switch start, end := strings.Index(string(t), "("), strings.LastIndex(string(t), ")"); {
	case len(t) == 0, start <= 0, end <= 0, end < start:
		return ""
	default:
		return t[start+1 : end]
	}
}

type Interface interface {
	Type() Type
	Rows() int
	RowValue(row int) interface{}
	ScanRow(dest interface{}, row int) error
	Append(v interface{}) error
	AppendRow(v interface{}) error
	Decode(decoder *binary.Decoder, rows int) error
	Encode(*binary.Encoder) error
}

type UnsupportedColumnType struct {
	t Type
}

func (u *UnsupportedColumnType) Type() Type                        { return u.t }
func (UnsupportedColumnType) Rows() int                            { return 0 }
func (u *UnsupportedColumnType) RowValue(row int) interface{}      { return nil }
func (u *UnsupportedColumnType) ScanRow(interface{}, int) error    { return u }
func (u *UnsupportedColumnType) Append(interface{}) error          { return u }
func (u *UnsupportedColumnType) AppendRow(interface{}) error       { return u }
func (u *UnsupportedColumnType) Decode(*binary.Decoder, int) error { return u }
func (u *UnsupportedColumnType) Encode(*binary.Encoder) error      { return u }

func (u *UnsupportedColumnType) Error() string {
	return fmt.Sprintf("unsupported column type %q", u.t)
}

var (
	_ error     = (*UnsupportedColumnType)(nil)
	_ Interface = (*UnsupportedColumnType)(nil)
)
