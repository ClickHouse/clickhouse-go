package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Tuple struct {
	chType  Type
	columns []Interface
}

func (col *Tuple) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var (
		element       []rune
		elements      []string
		brackets      int
		appendElement = func() {
			if len(element) != 0 {
				name := strings.TrimSpace(string(element))
				if parts := strings.SplitN(name, " ", 2); len(parts) == 2 {
					if !strings.Contains(parts[0], "(") {
						name = parts[1]
					}
				}
				elements = append(elements, name)
			}
		}
	)
	for _, r := range t.params() {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ',':
			if brackets == 0 {
				appendElement()
				element = element[:0]
				continue
			}
		}
		element = append(element, r)
	}
	appendElement()
	for _, ct := range elements {
		column, err := Type(strings.TrimSpace(ct)).Column()
		if err != nil {
			return nil, err
		}
		col.columns = append(col.columns, column)
	}
	if len(col.columns) != 0 {
		return col, nil
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}

func (col *Tuple) Type() Type {
	return col.chType
}

func (Tuple) ScanType() reflect.Type {
	return scanTypeSlice
}

func (col *Tuple) Rows() int {
	if len(col.columns) != 0 {
		return col.columns[0].Rows()
	}
	return 0
}

func (col *Tuple) Row(i int) interface{} {
	tuple := make([]interface{}, 0, len(col.columns))
	for _, c := range col.columns {
		tuple = append(tuple, c.Row(i))
	}
	return tuple
}

func (col *Tuple) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *[]interface{}:
		tuple := make([]interface{}, 0, len(col.columns))
		for _, c := range col.columns {
			tuple = append(tuple, c.Row(row))
		}
		*d = tuple
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: string(col.chType),
		}
	}
	return nil
}

func (col *Tuple) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case [][]interface{}:
		for _, v := range v {
			if err := col.AppendRow(v); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	return nil, &ColumnConverterErr{
		op:   "Append",
		to:   string(col.chType),
		from: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case []interface{}:
		if len(v) != len(col.columns) {
			return &BadSizeOfTuple{
				op:       "AppendRow",
				got:      len(v),
				expected: len(col.columns),
			}
		}
		for i, v := range v {
			if err := col.columns[i].AppendRow(v); err != nil {
				return err
			}
		}
		return nil
	}
	return &ColumnConverterErr{
		op:   "AppendRow",
		to:   string(col.chType),
		from: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) Decode(decoder *binary.Decoder, rows int) error {
	for _, c := range col.columns {
		if err := c.Decode(decoder, rows); err != nil {
			return err
		}
	}
	return nil
}

func (col *Tuple) Encode(encoder *binary.Encoder) error {
	for _, c := range col.columns {
		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

var _ Interface = (*Tuple)(nil)
