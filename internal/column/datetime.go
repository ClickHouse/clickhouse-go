package column

import (
	"time"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type DateTime struct {
	base
	isFull   bool
	timezone *time.Location
}

func (dt *DateTime) Read(decoder *binary.Decoder) (interface{}, error) {
	if dt.isFull {
		sec, err := decoder.Int32()
		if err != nil {
			return nil, err
		}
		return time.Unix(int64(sec), 0).In(dt.timezone), nil
	}
	sec, err := decoder.Int16()
	if err != nil {
		return nil, err
	}
	return time.Unix(int64(sec)*24*3600, 0).In(dt.timezone), nil
}

func (dt *DateTime) Write(encoder *binary.Encoder, v interface{}) error {
	var timestamp int64
	switch value := v.(type) {
	case time.Time:
		timestamp = value.Unix()
	case int16:
		timestamp = int64(value)
	case int32:
		timestamp = int64(value)
	case int64:
		timestamp = value
	case string:
		switch {
		case dt.isFull:
			tv, err := time.Parse("2006-01-02", value)
			if err != nil {
				return err
			}
			timestamp = time.Date(
				time.Time(tv).Year(),
				time.Time(tv).Month(),
				time.Time(tv).Day(),
				0, 0, 0, 0, time.UTC,
			).Unix()
		default:
			tv, err := time.Parse("2006-01-02 15:04:05", value)
			if err != nil {
				return err
			}
			timestamp = time.Date(
				time.Time(tv).Year(),
				time.Time(tv).Month(),
				time.Time(tv).Day(),
				time.Time(tv).Hour(),
				time.Time(tv).Minute(),
				time.Time(tv).Second(),
				0, time.UTC,
			).Unix()
		}
	}

	if dt.isFull {
		return encoder.Int32(int32(timestamp))
	}
	return encoder.Int16(int16(timestamp / 24 / 3600))
}
