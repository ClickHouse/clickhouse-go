package clickhouse

import (
	"regexp"
	"strings"
)

func isInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1])
	}
	return false
}

func isSelect(query string) bool {
	if f := strings.Fields(query); len(f) > 3 {
		return strings.EqualFold("SELECT", f[0])
	}
	return false
}

var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s+\(.*?\)`)

func formatQuery(query string) string {
	switch {
	case isInsert(query):
		return splitInsertRe.Split(query, -1)[0] + " FORMAT TabSeparated"
	case isSelect(query):
		return query + " FORMAT TabSeparatedWithNamesAndTypes"
	}
	return query
}
