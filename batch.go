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

// extractInsertSettingsMatch captures a trailing SETTINGS clause. The `\w+\s*=`
// after the SETTINGS keyword requires an actual `name = value` assignment so a table
// or column merely named "settings" is not mistaken for a settings clause.
var extractInsertSettingsMatch = regexp.MustCompile(`(?i)\s+(SETTINGS\s+\w+\s*=.+?)(?:\s+VALUES)?\s*$`)

func extractNormalizedInsertQueryAndColumns(query string) (normalizedQuery string, tableName string, columns []string, err error) {
	query = truncateFormat.ReplaceAllString(query, "")
	query = truncateValues.ReplaceAllString(query, "")

	// A SETTINGS clause may follow the optional column list, e.g.
	// "INSERT INTO t (a, b) SETTINGS async_insert=1". Capture it so it is preserved in
	// the normalized query sent to the server, and strip it from the query before the
	// table name and columns are extracted so it does not leak into either.
	var settingsClause string
	if loc := extractInsertSettingsMatch.FindStringSubmatchIndex(query); loc != nil {
		settingsClause = strings.TrimSpace(query[loc[2]:loc[3]])
		query = query[:loc[0]]
	}

	matches := normalizeInsertQueryMatch.FindStringSubmatch(query)
	if len(matches) == 0 {
		err = fmt.Errorf("invalid INSERT query: %s", query)
		return
	}

	normalizedQuery = matches[1]
	if settingsClause != "" {
		normalizedQuery += " " + settingsClause
	}
	normalizedQuery += " FORMAT Native"
	tableName = strings.TrimSpace(matches[2])

	columns = make([]string, 0)
	matches = extractInsertColumnsMatch.FindStringSubmatch(matches[1])
	if len(matches) == 2 {
		columns = strings.Split(matches[1], ",")
		for i := range columns {
			// refers to https://clickhouse.com/docs/en/sql-reference/syntax#identifiers
			// we can use identifiers with double quotes or backticks, for example: "id", `id`, but not both, like `"id"`.
			columns[i] = strings.ReplaceAll(strings.Trim(strings.TrimSpace(columns[i]), "\""), "`", "")
		}
	}

	return
}
