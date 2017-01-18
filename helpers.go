package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"
)

func isInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) && strings.Index(strings.ToUpper(query), " SELECT ") == -1
	}
	return false
}

func isSelect(query string) bool {
	if f := strings.Fields(query); len(f) > 3 {
		return strings.EqualFold("SELECT", f[0])
	}
	return false
}

var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)

func formatQuery(query string) string {
	if isInsert(query) {
		return splitInsertRe.Split(query, -1)[0] + " VALUES "
	}
	return query
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
