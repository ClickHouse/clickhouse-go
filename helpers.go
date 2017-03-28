package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

// Truncate timezone
//
//   clickhouse.Date(time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local)) -> time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
type Date time.Time

func (date Date) Value() (driver.Value, error) {
	return time.Date(time.Time(date).Year(), time.Time(date).Month(), time.Time(date).Day(), 0, 0, 0, 0, time.UTC), nil
}

// Truncate timezone
//
//   clickhouse.DateTime(time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local)) -> time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
type DateTime time.Time

func (datetime DateTime) Value() (driver.Value, error) {
	return time.Date(
		time.Time(datetime).Year(),
		time.Time(datetime).Month(),
		time.Time(datetime).Day(),
		time.Time(datetime).Hour(),
		time.Time(datetime).Minute(),
		time.Time(datetime).Second(),
		0,
		time.UTC,
	), nil
}

func numInput(query string) int {
	count := strings.Count(query, "?")
	args := make(map[string]struct{})
	for _, arg := range strings.Fields(query) {
		if strings.HasPrefix(arg, "@") {
			if _, found := args[arg]; !found {
				args[arg] = struct{}{}
				count++
			}
		}
	}
	return count
}

func isInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) && strings.Index(strings.ToUpper(query), " SELECT ") == -1
	}
	return false
}

func quote(v driver.Value) string {
	switch v.(type) {
	case string, *string, time.Time, *time.Time:
		return "'" + escape(v) + "'"
	}
	return fmt.Sprint(v)
}

func escape(v driver.Value) string {
	switch value := v.(type) {
	case string:
		return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value)
	case *string:
		return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(*value)
	case time.Time:
		return formatTime(value)
	case *time.Time:
		return formatTime(*value)
	}
	return fmt.Sprint(v)
}

func formatTime(value time.Time) string {
	if (value.Hour() + value.Minute() + value.Second() + value.Nanosecond()) == 0 {
		return value.Format("2006-01-02")
	}
	return value.Format("2006-01-02 15:04:05")
}

func toColumnType(ct string) (interface{}, error) {
	// PODs
	switch ct {
	case "Date":
		return Date{}, nil
	case "DateTime":
		return DateTime{}, nil
	case "String":
		return string(""), nil
	case "Int8":
		return int8(0), nil
	case "Int16":
		return int16(0), nil
	case "Int32":
		return int32(0), nil
	case "Int64":
		return int64(0), nil
	case "UInt8":
		return uint8(0), nil
	case "UInt16":
		return uint16(0), nil
	case "UInt32":
		return uint32(0), nil
	case "UInt64":
		return uint64(0), nil
	case "Float32":
		return float32(0), nil
	case "Float64":
		return float64(0), nil
	}

	// Specialised types
	switch {
	case strings.HasPrefix(ct, "FixedString"):
		var arrLen int
		if _, err := fmt.Sscanf(ct, "FixedString(%d)", &arrLen); err != nil {
			return nil, err
		}
		return make([]byte, arrLen), nil
	case strings.HasPrefix(ct, "Enum8"):
		enum, err := parseEnum(ct)
		if err != nil {
			return nil, err
		}
		return enum8(enum), nil
	case strings.HasPrefix(ct, "Enum16"):
		enum, err := parseEnum(ct)
		if err != nil {
			return nil, err
		}
		return enum16(enum), nil
	case strings.HasPrefix(ct, "Array"):
		if len(ct) < 11 {
			return nil, fmt.Errorf("invalid Array column type: %s", ct)
		}
		baseType, err := toColumnType(ct[6:][:len(ct)-7])
		if err != nil {
			return nil, fmt.Errorf("array: %v", err)
		}
		return array{
			baseType: baseType,
		}, nil
	}

	return nil, fmt.Errorf("unhandled type %v", ct)
}
