package column

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"time"
)

func ToInt64(i interface{}) (int64, bool) {
	if i == nil {
		return 0, true
	}
	switch i := i.(type) {
	case int:
		return int64(i), true
	case *int:
		return int64(*i), true
	case int8:
		return int64(i), true
	case *int8:
		return int64(*i), true
	case int16:
		return int64(i), true
	case *int16:
		return int64(*i), true
	case int32:
		return int64(i), true
	case *int32:
		return int64(*i), true
	case int64:
		return int64(i), true
	case *int64:
		return int64(*i), true
	case uint:
		return int64(i), true
	case *uint:
		return int64(*i), true
	case uint8:
		return int64(i), true
	case *uint8:
		return int64(*i), true
	case uint16:
		return int64(i), true
	case *uint16:
		return int64(*i), true
	case uint32:
		return int64(i), true
	case *uint32:
		return int64(*i), true
	case uint64:
		return int64(i), true
	case *uint64:
		return int64(*i), true
	case string:
		v, _ := strconv.ParseInt(i, 10, 64)
		return v, true
	case *string:
		v, _ := strconv.ParseInt(*i, 10, 64)
		return v, true
	case driver.Valuer:
		v, _ := i.Value()
		return ToInt64(v)
	case *driver.Valuer:
		v, _ := (*i).Value()
		return ToInt64(v)
	case json.Number:
		return ToInt64(string(i))
	case *json.Number:
		return ToInt64(string(*i))
	case time.Duration:
		return int64(i), true
	case *time.Duration:
		return int64(*i), true
	case bool:
		if i {
			return 1, true
		}
		return 0, true
	case *bool:
		if *i {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

func ToUInt64(i interface{}) (uint64, bool) {
	if i == nil {
		return 0, true
	}
	switch i := i.(type) {
	case int:
		return uint64(i), true
	case *int:
		return uint64(*i), true
	case int8:
		return uint64(i), true
	case *int8:
		return uint64(*i), true
	case int16:
		return uint64(i), true
	case *int16:
		return uint64(*i), true
	case int32:
		return uint64(i), true
	case *int32:
		return uint64(*i), true
	case int64:
		return uint64(i), true
	case *int64:
		return uint64(*i), true
	case uint:
		return uint64(i), true
	case *uint:
		return uint64(*i), true
	case uint8:
		return uint64(i), true
	case *uint8:
		return uint64(*i), true
	case uint16:
		return uint64(i), true
	case *uint16:
		return uint64(*i), true
	case uint32:
		return uint64(i), true
	case *uint32:
		return uint64(*i), true
	case uint64:
		return i, true
	case *uint64:
		return *i, true
	case string:
		v, _ := strconv.ParseUint(i, 10, 64)
		return v, true
	case *string:
		v, _ := strconv.ParseUint(*i, 10, 64)
		return v, true
	case driver.Valuer:
		v, _ := i.Value()
		return ToUInt64(v)
	case *driver.Valuer:
		v, _ := (*i).Value()
		return ToUInt64(v)
	case json.Number:
		return ToUInt64(string(i))
	case *json.Number:
		return ToUInt64(string(*i))
	case time.Duration:
		return uint64(i), true
	case *time.Duration:
		return uint64(*i), true
	case bool:
		if i {
			return 1, true
		}
		return 0, true
	case *bool:
		if *i {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}
