package clickhouse

import (
	"bytes"
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
	var (
		count          int
		args           = make(map[string]struct{})
		reader         = bytes.NewReader([]byte(query))
		quote, keyword bool
	)
	for {
		if char, _, err := reader.ReadRune(); err == nil {
			switch char {
			case '\'', '`':
				quote = !quote
			}
			if quote {
				continue
			}
			switch {
			case char == '?' && keyword:
				count++
			case char == '@':
				if param := paramParser(reader); len(param) != 0 {
					if _, found := args[param]; !found {
						args[param] = struct{}{}
						count++
					}
				}
			case
				char == '=',
				char == '<',
				char == '>',
				char == '(',
				char == ',',
				char == '%':
				keyword = true
			default:
				keyword = keyword && (char == ' ' || char == '\t' || char == '\n')
			}
		} else {
			break
		}
	}
	return count
}

func paramParser(reader *bytes.Reader) string {
	var name bytes.Buffer
	for {
		if char, _, err := reader.ReadRune(); err == nil {
			if char == '_' || char >= '0' && char <= '9' || 'a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' {
				name.WriteRune(char)
			} else {
				reader.UnreadRune()
				break
			}
		} else {
			break
		}
	}
	return name.String()
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

	return nil, fmt.Errorf("func toColumnType: unhandled type %v", ct)
}
