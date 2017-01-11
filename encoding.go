package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

func encode(v driver.Value) string {
	switch value := v.(type) {
	case string:
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
	case *string:
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(*value) + "'"
	case time.Time:
		return encodeTime(value)
	case *time.Time:
		return encodeTime(*value)
	}
	return fmt.Sprint(v)
}

func encodeTime(value time.Time) string {
	if (value.Hour() + value.Minute() + value.Second() + value.Nanosecond()) == 0 {
		return "'" + value.Format("2006-01-02") + "'"
	}
	return "'" + value.Format("2006-01-02 15:04:05") + "'"
}

func decode(t string, v []byte) (driver.Value, error) {
	switch {
	case t == "Date":
		date, err := time.Parse("2006-01-02", string(v))
		if err != nil {
			return nil, err
		}
		return date.UTC(), nil
	case t == "DateTime":
		datetime, err := time.Parse("2006-01-02 15:04:05", string(v))
		if err != nil {
			return nil, err
		}
		return datetime.UTC(), nil
	case t == "String" || strings.HasPrefix("FixedString", t):
		return bytes.Replace(bytes.Replace(v, []byte(`\\`), []byte(`\`), -1), []byte(`\'`), []byte(`'`), -1), nil
	}
	return v, nil
}
