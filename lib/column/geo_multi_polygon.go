package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/paulmach/orb"
)

type MultiPolygon struct {
	set *Array
}

func (col *MultiPolygon) Type() Type {
	return "MultiPolygon"
}

func (col *MultiPolygon) ScanType() reflect.Type {
	return scanTypeMultiPolygon
}

func (col *MultiPolygon) Rows() int {
	return col.set.Rows()
}

func (col *MultiPolygon) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *MultiPolygon) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *orb.MultiPolygon:
		*d = col.row(row)
	case **orb.MultiPolygon:
		*d = new(orb.MultiPolygon)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "MultiPolygon",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *MultiPolygon) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []orb.MultiPolygon:
		values := make([][]orb.Polygon, 0, len(v))
		for _, v := range v {
			values = append(values, v)
		}
		return col.set.Append(values)

	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "MultiPolygon",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *MultiPolygon) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case orb.MultiPolygon:
		return col.set.AppendRow([]orb.Polygon(v))
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "MultiPolygon",
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (col *MultiPolygon) Decode(decoder *binary.Decoder, rows int) error {
	return col.set.Decode(decoder, rows)
}

func (col *MultiPolygon) Encode(encoder *binary.Encoder) error {
	return col.set.Encode(encoder)
}

func (col *MultiPolygon) row(i int) orb.MultiPolygon {
	var value []orb.Polygon
	{
		col.set.ScanRow(&value, i)
	}
	return value
}

var _ Interface = (*MultiPolygon)(nil)
