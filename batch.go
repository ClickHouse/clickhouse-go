package clickhouse

import (
	"fmt"
	"regexp"
	"strings"
)

var normalizeInsertQueryMatch = regexp.MustCompile(`(?i)(?:(?:--[^\n]*|#![^\n]*|#\s[^\n]*)\n\s*)*(INSERT\s+INTO\s+([^(]+)(?:\s*\([^()]*(?:\([^()]*\)[^()]*)*\))?)(?:\s*VALUES)?`)
var truncateFormat = regexp.MustCompile(`(?i)\sFORMAT\s+[^\s]+`)
var truncateValues = regexp.MustCompile(`\sVALUES\s.*$`)
var extractInsertColumnsMatch = regexp.MustCompile(`(?si)INSERT INTO .+\s\((?P<Columns>.+)\)$`)

func splitColumnsRespectingQuotes(columnsStr string) []string {
	var columns []string
	var current strings.Builder
	inBacktick := false
	inDoubleQuote := false

	for i := 0; i < len(columnsStr); i++ {
		c := columnsStr[i]

		if c == '`' && !inDoubleQuote {
			inBacktick = !inBacktick
			current.WriteByte(c)
		} else if c == '"' && !inBacktick {
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(c)
		} else if c == ',' && !inBacktick && !inDoubleQuote {
			columns = append(columns, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		columns = append(columns, strings.TrimSpace(current.String()))
	}

	return columns
}

func extractNormalizedInsertQueryAndColumns(query string) (normalizedQuery string, tableName string, columns []string, err error) {
	query = truncateFormat.ReplaceAllString(query, "")
	query = truncateValues.ReplaceAllString(query, "")

	matches := normalizeInsertQueryMatch.FindStringSubmatch(query)
	if len(matches) == 0 {
		err = fmt.Errorf("invalid INSERT query: %s", query)
		return
	}

	normalizedQuery = fmt.Sprintf("%s FORMAT Native", matches[1])
	tableName = strings.TrimSpace(matches[2])

	columns = make([]string, 0)
	matches = extractInsertColumnsMatch.FindStringSubmatch(matches[1])
	if len(matches) == 2 {
		rawColumns := splitColumnsRespectingQuotes(matches[1])
		for _, col := range rawColumns {
			columns = append(columns, strings.ReplaceAll(strings.Trim(strings.TrimSpace(col), "\""), "`", ""))
		}
	}

	return
}
