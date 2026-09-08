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
// or column merely named "settings" is not mistaken for a settings clause. The `s` flag
// lets the clause span newlines, so a settings list written over several lines is
// captured as a whole. Trailing `;` statement terminators and whitespace are matched
// outside the capture group so they are not folded into the clause and do not leak into
// the normalized query as "SETTINGS ...; FORMAT Native". Only terminators at the very
// end of the query are consumed, so a `;` inside a quoted setting value is preserved.
// Comments and a bare trailing VALUES keyword are removed from the capture by
// normalizeSettingsClause.
var extractInsertSettingsMatch = regexp.MustCompile(`(?is)\s+(SETTINGS\s+\w+\s*=.+?)[\s;]*$`)

// truncateTrailingValues matches a VALUES keyword with no rows after it, which
// truncateValues leaves in place.
var truncateTrailingValues = regexp.MustCompile(`(?is)\s+VALUES[\s;]*$`)

// truncateLeadingComments matches the single line comments a statement may be prefixed
// with, using the same markers as normalizeInsertQueryMatch.
var truncateLeadingComments = regexp.MustCompile(`\A\s*(?:(?:--|#!|#\s)[^\n]*\n\s*)*`)

func extractNormalizedInsertQueryAndColumns(query string) (normalizedQuery string, tableName string, columns []string, err error) {
	insertStmt, tableName, columns, err := extractInsertQueryComponents(query)
	if err != nil {
		return "", "", nil, err
	}
	return fmt.Sprintf("%s FORMAT Native", insertStmt), tableName, columns, nil
}

// extractInsertQueryComponents strips any FORMAT clause or VALUES suffix from
// an INSERT query and returns the bare statement, so the caller can append the
// FORMAT of its choosing.
func extractInsertQueryComponents(query string) (insertStmt string, tableName string, columns []string, err error) {
	query = truncateFormat.ReplaceAllString(query, "")
	query = truncateValues.ReplaceAllString(query, "")

	// Comments in front of the statement are not part of it. They are dropped before the
	// SETTINGS clause is located so a comment that mentions a settings assignment is not
	// mistaken for the clause.
	query = query[len(truncateLeadingComments.FindString(query)):]

	// A SETTINGS clause may follow the optional column list, e.g.
	// "INSERT INTO t (a, b) SETTINGS async_insert=1". Capture it so it is preserved in
	// the normalized query sent to the server, and strip it from the query before the
	// table name and columns are extracted so it does not leak into either.
	var settingsClause string
	if loc := extractInsertSettingsMatch.FindStringSubmatchIndex(query); loc != nil {
		settingsClause = normalizeSettingsClause(query[loc[2]:loc[3]])
		query = query[:loc[0]]
	}

	matches := normalizeInsertQueryMatch.FindStringSubmatch(query)
	if len(matches) == 0 {
		err = fmt.Errorf("invalid INSERT query: %s", query)
		return
	}

	insertStmt = matches[1]
	if settingsClause != "" {
		insertStmt += " " + settingsClause
	}
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

// normalizeSettingsClause removes comments, a bare trailing VALUES keyword and trailing
// statement terminators from a captured SETTINGS clause. A comment left inside the
// clause would comment out the FORMAT clause the caller appends after it, so the server
// would fall back to its default input format. Comment markers inside a quoted value or
// identifier are kept.
func normalizeSettingsClause(clause string) string {
	var sb strings.Builder
	var quote byte
	for i := 0; i < len(clause); i++ {
		c := clause[i]
		if quote != 0 {
			sb.WriteByte(c)
			switch {
			case c == '\\' && i+1 < len(clause):
				i++
				sb.WriteByte(clause[i])
			case c == quote:
				quote = 0
			}
			continue
		}

		switch {
		case c == '\'', c == '"', c == '`':
			quote = c
			sb.WriteByte(c)
		case isLineCommentStart(clause[i:]):
			for i+1 < len(clause) && clause[i+1] != '\n' {
				i++
			}
		case strings.HasPrefix(clause[i:], "/*"):
			if end := strings.Index(clause[i+2:], "*/"); end >= 0 {
				i += 2 + end + 1
			} else {
				i = len(clause)
			}
		default:
			sb.WriteByte(c)
		}
	}

	clause = truncateTrailingValues.ReplaceAllString(sb.String(), "")

	return strings.TrimSpace(strings.TrimRight(clause, " \t\r\n;"))
}

// isLineCommentStart reports whether s begins with a single line comment marker, using
// the same markers as normalizeInsertQueryMatch: "--", "#!" and "#" followed by
// whitespace. A "#" at the very end of the clause also starts a comment, because the
// caller appends " FORMAT ..." after it.
func isLineCommentStart(s string) bool {
	switch {
	case strings.HasPrefix(s, "--"), strings.HasPrefix(s, "#!"):
		return true
	case strings.HasPrefix(s, "#"):
		return len(s) == 1 || s[1] == ' ' || s[1] == '\t' || s[1] == '\r' || s[1] == '\n'
	default:
		return false
	}
}
