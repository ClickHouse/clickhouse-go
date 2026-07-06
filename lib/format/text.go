package format

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

const (
	dateLayout     = "2006-01-02"
	dateTimeLayout = "2006-01-02 15:04:05"
)

var timeType = reflect.TypeOf(time.Time{})

// timeLayout returns the ClickHouse text rendering layout for a date/time
// column type. DateTime64 columns render their fractional seconds according
// to the type's scale parameter.
func timeLayout(t column.Type) string {
	s := string(t)
	switch {
	case strings.HasPrefix(s, "DateTime64"):
		scale := 3
		if params := strings.TrimSuffix(strings.TrimPrefix(s, "DateTime64("), ")"); params != "" {
			if n, err := strconv.Atoi(strings.TrimSpace(strings.Split(params, ",")[0])); err == nil {
				scale = n
			}
		}
		if scale <= 0 {
			return dateTimeLayout
		}
		if scale > 9 {
			scale = 9
		}
		return dateTimeLayout + "." + strings.Repeat("0", scale)
	case strings.HasPrefix(s, "DateTime"):
		return dateTimeLayout
	case strings.HasPrefix(s, "Date"):
		return dateLayout
	default:
		return dateTimeLayout
	}
}

// rowValue returns the value at row with any pointer indirection removed
// (Nullable columns return pointers for non-NULL values). The second return
// value reports whether the value is NULL.
func rowValue(col column.Interface, row int) (any, bool) {
	v := col.Row(row, false)
	if v == nil {
		return nil, true
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, true
		}
		return rv.Elem().Interface(), false
	}
	return v, false
}

// renderText renders the value at row as ClickHouse text-format output.
// The second return value reports whether the value is NULL.
func renderText(col column.Interface, row int) (string, bool) {
	v, isNull := rowValue(col, row)
	if isNull {
		return "", true
	}
	switch v := v.(type) {
	case string:
		return v, false
	case bool:
		if v {
			return "true", false
		}
		return "false", false
	case time.Time:
		return v.Format(timeLayout(col.Type())), false
	case int8:
		return strconv.FormatInt(int64(v), 10), false
	case int16:
		return strconv.FormatInt(int64(v), 10), false
	case int32:
		return strconv.FormatInt(int64(v), 10), false
	case int64:
		return strconv.FormatInt(v, 10), false
	case uint8:
		return strconv.FormatUint(uint64(v), 10), false
	case uint16:
		return strconv.FormatUint(uint64(v), 10), false
	case uint32:
		return strconv.FormatUint(uint64(v), 10), false
	case uint64:
		return strconv.FormatUint(v, 10), false
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), false
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), false
	default:
		// UUID, Decimal, IP and similar types have faithful String forms.
		return fmt.Sprintf("%v", v), false
	}
}

// appendText parses s as ClickHouse text-format input and appends it to col.
// Numeric columns require explicit parsing: column.Interface.AppendRow does
// not convert from string.
func appendText(col column.Interface, s string) error {
	st := col.ScanType()
	if st == nil {
		return col.AppendRow(s)
	}
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}
	if st == timeType {
		t, err := parseTextTime(col.Type(), s)
		if err != nil {
			return err
		}
		return col.AppendRow(t)
	}
	switch st.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, st.Bits())
		if err != nil {
			return fmt.Errorf("parse %s: %w", col.Type(), err)
		}
		return col.AppendRow(reflect.ValueOf(n).Convert(st).Interface())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, st.Bits())
		if err != nil {
			return fmt.Errorf("parse %s: %w", col.Type(), err)
		}
		return col.AppendRow(reflect.ValueOf(n).Convert(st).Interface())
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, st.Bits())
		if err != nil {
			return fmt.Errorf("parse %s: %w", col.Type(), err)
		}
		return col.AppendRow(reflect.ValueOf(f).Convert(st).Interface())
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return fmt.Errorf("parse %s: %w", col.Type(), err)
		}
		return col.AppendRow(b)
	case reflect.String:
		return col.AppendRow(s)
	default:
		// String-accepting columns (UUID, Decimal, IP, Enum, ...) convert on
		// AppendRow; anything else surfaces its own conversion error.
		return col.AppendRow(s)
	}
}

func parseTextTime(t column.Type, s string) (time.Time, error) {
	layout := dateTimeLayout
	if strings.HasPrefix(string(t), "Date") && !strings.HasPrefix(string(t), "DateTime") {
		layout = dateLayout
	}
	// time.Parse accepts a fractional second after the seconds field even when
	// the layout carries none, so a single layout covers DateTime and DateTime64.
	parsed, err := time.Parse(layout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %s: %w", t, err)
	}
	return parsed, nil
}
