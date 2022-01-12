package column

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Interval struct {
	chType Type
	values Int64
}

func (col *Interval) parse(t Type) (Interface, error) {
	switch col.chType = t; col.chType {
	case "IntervalSecond", "IntervalMinute", "IntervalHour", "IntervalDay", "IntervalWeek", "IntervalMonth", "IntervalYear":
		return col, nil
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}

func (col *Interval) Type() Type { return col.chType }
func (col *Interval) Rows() int  { return len(col.values) }
func (col *Interval) RowValue(row int) interface{} {
	return col.row(row)
}
func (col *Interval) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row)
	case **string:
		*d = new(string)
		**d = col.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Interval",
		}
	}
	return nil
}

func (Interval) Append(v interface{}) ([]uint8, error) {
	return nil, &StoreSpecialDataType{"Interval"}
}

func (Interval) AppendRow(v interface{}) error {
	return &StoreSpecialDataType{"Interval"}
}

func (col *Interval) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (Interval) Encode(encoder *binary.Encoder) error { return &StoreSpecialDataType{"Interval"} }

func (col *Interval) row(i int) string {
	if col.values[i] > 1 {
		return fmt.Sprintf("%d %s", col.values[i], strings.TrimPrefix(string(col.chType), "Interval")) + "s"
	}
	return fmt.Sprintf("%d %s", col.values[i], strings.TrimPrefix(string(col.chType), "Interval"))
}

var _ Interface = (*Interval)(nil)
