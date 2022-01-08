package column

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type DateTime []int32

func (dt *DateTime) Rows() int {
	return len(*dt)
}

func (dt *DateTime) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < int(rows); i++ {
		v, err := decoder.Int32()
		if err != nil {
			return err
		}
		*dt = append(*dt, v)
	}
	return nil
}

func (dt *DateTime) RowValue(row int) interface{} {
	value := *dt
	return time.Unix(int64(value[row]), 0)
}

func (dt *DateTime) ScanRow(dest interface{}, row int) error {
	v := *dt
	switch d := dest.(type) {
	case *time.Time:
		*d = time.Unix(int64(v[row]), 0)
	case **time.Time:
		*d = new(time.Time)
		**d = time.Unix(int64(v[row]), 0)
	}
	return nil
}

func (dt *DateTime) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		*dt = append(*dt, int32(v.Unix()))
	case null:
		*dt = append(*dt, 0)
	}
	return nil
}

func (dt *DateTime) Encode(encoder *binary.Encoder) error {
	for _, v := range *dt {
		if err := encoder.Int32(v); err != nil {
			return err
		}
	}
	return nil
}

var _ Interface = (*DateTime)(nil)
