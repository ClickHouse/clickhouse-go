package clickhouse

import "strings"

func isInsert(query string) (string, bool) {
	if f := strings.Fields(query); len(f) > 2 && strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) {
		return f[2], true
	}
	return "", false
}
