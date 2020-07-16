package column

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type DateTime struct {
	base
	Timezone *time.Location
}

func (dt *DateTime) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	sec, err := decoder.Int32()
	if err != nil {
		return nil, err
	}
	return time.Unix(int64(sec), 0).In(dt.Timezone), nil
}

func (dt *DateTime) Write(encoder *binary.Encoder, v interface{}) error {
	var timestamp int64
	switch value := v.(type) {
	case time.Time:
		if !value.IsZero() {
			timestamp = value.Unix()
		}
	case int16:
		timestamp = int64(value)
	case int32:
		timestamp = int64(value)
	case uint32:
		timestamp = int64(value)
	case uint64:
		timestamp = int64(value)
	case int64:
		timestamp = value
	case string:
		var err error
		timestamp, err = dt.parse(value)
		if err != nil {
			return err
		}

	case *time.Time:
		if value != nil && !(*value).IsZero() {
			timestamp = (*value).Unix()
		}
	case *int16:
		timestamp = int64(*value)
	case *int32:
		timestamp = int64(*value)
	case *int64:
		timestamp = *value
	case *string:
		var err error
		timestamp, err = dt.parse(*value)
		if err != nil {
			return err
		}

	default:
		return &ErrUnexpectedType{
			T:      v,
			Column: dt,
		}
	}

	return encoder.Int32(int32(timestamp))
}

func (dt *DateTime) parse(value string) (int64, error) {
	var year, month, day, hour, minute, second int
	_, err := fmt.Sscanf(value, "%d-%d-%d %d:%d:%d",
		&year, &month, &day, &hour, &minute, &second)
	if err != nil {
		return 0, err
	}

	if year == 0 {
		return 0, nil
	}

	seconds := makeDaySecond(year, month, day, time.Local) +
		int64(hour*3600+minute*60+second)
	return seconds, nil
}

const (
	DATE_LUT_MIN_YEAR = 1970
	DATE_LUT_MAX_YEAR = 2106
)

func makeDaySecond(year, month, day int, loc *time.Location) int64 {
	switch {
	case year < DATE_LUT_MIN_YEAR:
	case year > DATE_LUT_MAX_YEAR:
	case month < 1:
	case month > 12:
	case day < 1:
	case day > 31:
	default:
		//use local timzone when insert into clickhouse
		return time.Date(year, time.Month(month), day,
			0, 0, 0, 0, loc).Unix()
	}
	return 0
}
