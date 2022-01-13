package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

type DateTime64 struct {
	chType    Type
	values    Int64
	timezone  *time.Location
	precision int
}

func (dt *DateTime64) parse(t Type) (_ Interface, err error) {
	dt.chType = t
	switch params := strings.Split(t.params(), ","); len(params) {
	case 2:
		if dt.precision, err = strconv.Atoi(params[0]); err != nil {
			return nil, err
		}
		if dt.timezone, err = timezone.Load(params[1][2 : len(params[1])-1]); err != nil {
			return nil, err
		}
	case 1:
		if dt.precision, err = strconv.Atoi(params[0]); err != nil {
			return nil, err
		}
	default:
		return &UnsupportedColumnType{
			t: t,
		}, nil
	}
	return dt, nil
}

func (dt *DateTime64) Type() Type {
	return dt.chType
}

func (col *DateTime64) ScanType() reflect.Type {
	return scanTypeTime
}

func (dt *DateTime64) Rows() int {
	return len(dt.values)
}

func (dt *DateTime64) Row(i int) interface{} {
	return dt.row(i)
}

func (dt *DateTime64) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = dt.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = dt.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Datetime64",
		}
	}
	return nil
}

func (dt *DateTime64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		dt.values = append(dt.values, dt.timeToInt64(v))
	case *time.Time:
		switch {
		case v == nil:
			dt.values = append(dt.values, dt.timeToInt64(*v))
		default:
			dt.values = append(dt.values, 0)
		}
	case null:
		dt.values = append(dt.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Datetime64",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (dt *DateTime64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int64, 0, len(v))
		for _, t := range v {
			in = append(in, dt.timeToInt64(t))
		}
		dt.values, nulls = append(dt.values, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				dt.values = append(dt.values, dt.timeToInt64(*v))
			default:
				dt.values, nulls[i] = append(dt.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Datetime64",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *DateTime64) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *DateTime64) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *DateTime64) row(i int) time.Time {
	var nano int64
	if dt.precision < 19 {
		nano = dt.values[i] * int64(math.Pow10(9-dt.precision))
	}
	var (
		sec  = nano / int64(10e8)
		nsec = nano - sec*10e8
		time = time.Unix(sec, nsec)
	)
	if dt.timezone != nil {
		time = time.In(dt.timezone)
	}
	return time
}

func (dt *DateTime64) timeToInt64(t time.Time) int64 {
	var timestamp int64
	if !t.IsZero() {
		timestamp = t.UnixNano()
	}
	return timestamp / int64(math.Pow10(9-dt.precision))
}

var _ Interface = (*DateTime64)(nil)
