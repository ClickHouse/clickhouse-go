package column

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
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

func (t Type) IsArray() bool {
	return strings.HasPrefix(string(t), "Array")
}

func (t Type) IsEnum() bool {
	return strings.HasPrefix(string(t), "Enum8") || strings.HasPrefix(string(t), "Enum16")
}

func (t Type) IsNullable() bool {
	return strings.HasPrefix(string(t), "Nullable")
}

type Interface interface {
	Rows() int
	ScanRow(dest interface{}, row int) error
	AppendRow(v interface{}) error
	//Append(v interface{}) error
	Decode(decoder *binary.Decoder, rows int) error
	Encode(*binary.Encoder) error
}

type Undefined struct{}

func (Undefined) Rows() int                         { return 0 }
func (Undefined) ScanRow(interface{}, int) error    { return fmt.Errorf("undefined") }
func (Undefined) AppendRow(interface{}) error       { return fmt.Errorf("undefined") }
func (Undefined) Decode(*binary.Decoder, int) error { return fmt.Errorf("undefined") }
func (Undefined) Encode(*binary.Encoder) error      { return fmt.Errorf("undefined") }

var _ Interface = (*Undefined)(nil)
