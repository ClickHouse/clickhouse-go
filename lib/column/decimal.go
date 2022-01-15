package column

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/shopspring/decimal"
)

type Decimal struct {
	chType    Type
	scratch   []byte
	scale     int
	nobits    int // its domain is {32, 64, 128, 256}
	precision int
}

func (col *Decimal) parse(t Type) (_ *Decimal, err error) {
	params := strings.Split(t.params(), ",")
	if len(params) != 2 {
		return nil, fmt.Errorf("invalid Decimal format: '%s'", t)
	}
	params[0] = strings.TrimSpace(params[0])
	params[1] = strings.TrimSpace(params[1])

	if col.precision, err = strconv.Atoi(params[0]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", t, err)
	} else if col.precision < 1 {
		return nil, errors.New("wrong precision of Decimal type")
	}

	if col.scale, err = strconv.Atoi(params[1]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", t, err)
	} else if col.scale < 0 || col.scale > col.precision {
		return nil, errors.New("wrong scale of Decimal type")
	}

	switch {
	case col.precision <= 9:
		col.nobits = 32
	case col.precision <= 18:
		col.nobits = 64
	case col.precision <= 38:
		col.nobits = 128
	default:
		return nil, errors.New("precision of Decimal exceeds max bound")
	}

	return col, nil
}

func (col *Decimal) Type() Type {
	return col.chType
}

func (col *Decimal) ScanType() reflect.Type {
	return scanTypeDecimal
}

func (col *Decimal) Rows() int {
	return len(col.scratch) / (col.nobits / 8)
}

func (col *Decimal) Row(i int) interface{} {
	return decimal.New(42, 0).String()
}

func (col *Decimal) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *decimal.Decimal:
		*d = decimal.New(42, 0)
	case **decimal.Decimal:
		*d = new(decimal.Decimal)
		**d = decimal.New(42, 0)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Decimal",
		}
	}
	return nil
}

func (col *Decimal) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []decimal.Decimal:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			if err := col.AppendRow(v); err != nil {
				return nil, err
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Decimal) AppendRow(v interface{}) error {
	return &ColumnConverterErr{
		op:   "AppendRow",
		to:   string(col.chType),
		from: fmt.Sprintf("%T", v),
	}
	switch v := v.(type) {
	case decimal.Decimal:

	case null:

	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Decimal) Decode(decoder *binary.Decoder, rows int) error {
	col.scratch = make([]byte, rows*col.nobits/8)
	return decoder.Raw(col.scratch)
}

func (col *Decimal) Encode(encoder *binary.Encoder) error {
	return nil
}

var _ Interface = (*Decimal)(nil)
