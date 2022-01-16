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
	scale     int
	nobits    int // its domain is {32, 64, 128, 256}
	precision int
	values    []decimal.Decimal
}

func (col *Decimal) parse(t Type) (_ *Decimal, err error) {
	col.chType = t
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
		/*	case col.precision <= 38:
			col.nobits = 128*/
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
	return len(col.values)
}

func (col *Decimal) Row(i int) interface{} {
	return col.values[i]
}

func (col *Decimal) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *decimal.Decimal:
		*d = col.values[row]
	case **decimal.Decimal:
		*d = new(decimal.Decimal)
		**d = col.values[row]
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
		col.values, nulls = append(col.values, v...), make([]uint8, len(v))
	case []*decimal.Decimal:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v == nil:
				col.values, nulls[i] = append(col.values, decimal.New(0, 0)), 0
			default:
				col.values = append(col.values, *v)
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
	switch v := v.(type) {
	case decimal.Decimal:
		col.values = append(col.values, v)
	case null:
		col.values = append(col.values, decimal.New(0, 0))
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
	switch col.nobits {
	case 32:
		var base UInt32
		if err := base.Decode(decoder, rows); err != nil {
			return err
		}
		for _, v := range base {
			col.values = append(col.values, decimal.New(int64(v), int32(-col.scale)))
		}
	case 64:
		var base UInt64
		if err := base.Decode(decoder, rows); err != nil {
			return err
		}
		for _, v := range base {
			col.values = append(col.values, decimal.New(int64(v), int32(-col.scale)))
		}
	default:
		return fmt.Errorf("unsupported %s", col.chType)
	}
	return nil
}

func (col *Decimal) Encode(encoder *binary.Encoder) error {
	switch col.nobits {
	case 32:
		var base UInt32
		for _, v := range col.values {
			int := v.IntPart()
			if v.Exponent() != int32(col.scale) {
				int = decimal.NewFromBigInt(v.Coefficient(), v.Exponent()+int32(col.scale)).IntPart()
			}
			base = append(base, uint32(int))
		}
		return base.Encode(encoder)
	case 64:
		var base UInt64
		for _, v := range col.values {
			int := v.IntPart()
			if v.Exponent() != int32(col.scale) {
				int = decimal.NewFromBigInt(v.Coefficient(), v.Exponent()+int32(col.scale)).IntPart()
			}
			base = append(base, uint64(int))
		}
		return base.Encode(encoder)
	}
	return fmt.Errorf("unsupported %s", col.chType)
}

var _ Interface = (*Decimal)(nil)
