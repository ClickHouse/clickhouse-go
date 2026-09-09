package clickhouse

import (
	"fmt"
	"regexp"
	"strings"
)

// normalizeInsertQueryMatch captures the INSERT statement and its table name. It runs on
// a query sanitizeInsertQuery has already stripped of comments, so it does not have to
// skip them itself.
var normalizeInsertQueryMatch = regexp.MustCompile(`(?i)(INSERT\s+INTO\s+([^(]+)(?:\s*\([^()]*(?:\([^()]*\)[^()]*)*\))?)(?:\s*VALUES)?`)
var extractInsertColumnsMatch = regexp.MustCompile(`(?si)INSERT INTO .+\s\((?P<Columns>.+)\)$`)

// extractInsertSettingsMatch captures a trailing SETTINGS clause. The `\w+\s*=`
// after the SETTINGS keyword requires an actual `name = value` assignment so a table
// or column merely named "settings" is not mistaken for a settings clause. The `s` flag
// lets the clause span newlines, so a settings list written over several lines is
// captured as a whole. Trailing `;` statement terminators and whitespace are matched
// outside the capture group so they are not folded into the clause and do not leak into
// the normalized query as "SETTINGS ...; FORMAT Native". Only terminators at the very
// end of the query are consumed, so a `;` inside a quoted setting value is preserved.
// The keyword only has to start a word, not to be preceded by whitespace, because
// ClickHouse also accepts a clause written directly after the column list, as in
// "INSERT INTO t (a, b)SETTINGS async_insert=1". The caller pairs the match with
// isKeyword, which limits what may precede the keyword to a token boundary or that
// closing parenthesis, so a "SETTINGS name=value" text inside a quoted value or an
// identifier is not taken for a clause. Comments, the FORMAT clause and everything from
// a VALUES keyword on are removed from the query by sanitizeInsertQuery before the
// clause is captured.
var extractInsertSettingsMatch = regexp.MustCompile(`(?is)\s*\b(SETTINGS\s+\w+\s*=.+?)[\s;]*$`)

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
	sanitized := sanitizeInsertQuery(query)

	// A SETTINGS clause may follow the optional column list, e.g.
	// "INSERT INTO t (a, b) SETTINGS async_insert=1". Capture it so it is preserved in
	// the normalized query sent to the server, and strip it from the query before the
	// table name and columns are extracted so it does not leak into either.
	var settingsClause string
	if loc := extractInsertSettingsMatch.FindStringSubmatchIndex(sanitized); loc != nil &&
		isKeyword(sanitized[loc[2]:], []byte(sanitized[:loc[2]]), settingsKeyword) {
		settingsClause = sanitized[loc[2]:loc[3]]
		sanitized = sanitized[:loc[0]]
	}

	matches := normalizeInsertQueryMatch.FindStringSubmatch(sanitized)
	if len(matches) == 0 {
		// The query as given by the caller is reported, not the sanitized one, so the
		// error still shows what was passed in.
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

// sanitizeInsertQuery removes the parts of an INSERT statement that the batch does not
// send to the server: comments, and everything from a VALUES or a FORMAT keyword on,
// which is the row data a batch sends in the request body instead, together with the
// FORMAT clause the caller replaces with its own. A comment left in the statement would
// comment out the FORMAT clause appended after it, and a comment holding a settings
// assignment would otherwise be taken for a real SETTINGS clause.
//
// The scan is quote aware, so a comment marker, a VALUES keyword or a FORMAT keyword
// inside a quoted value, an identifier or a heredoc is kept. The VALUES and FORMAT
// keywords are only recognized outside a parenthesized list and not directly after INTO,
// so a table or column named "values" or "format" is kept as well.
func sanitizeInsertQuery(query string) string {
	out := make([]byte, 0, len(query))
	var quote byte
	depth := 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		if quote != 0 {
			out = append(out, c)
			switch {
			case c == '\\' && i+1 < len(query):
				i++
				out = append(out, query[i])
			case c == quote:
				quote = 0
			}
			continue
		}

		if c == '\'' || c == '"' || c == '`' {
			quote = c
			out = append(out, c)
			continue
		}

		if end := commentEnd(query[i:]); end > 0 {
			i += end - 1
			out = appendCommentSeparator(out, query[i+1:])
			continue
		}

		if end := heredocEnd(query[i:]); end > 0 {
			out = append(out, query[i:i+end]...)
			i += end - 1
			continue
		}

		if depth == 0 && !followsInto(out) {
			if isKeyword(query[i:], out, valuesKeyword) {
				// Everything from an unquoted VALUES keyword on is row data.
				break
			}
			if formatClauseEnd(query[i:], out) > 0 {
				// Everything from a FORMAT clause on is row data written in the format
				// it names. The caller appends the FORMAT of its choosing instead.
				break
			}
		}

		switch {
		case c == '(':
			depth++
		case c == ')' && depth > 0:
			depth--
		}
		out = append(out, c)
	}

	// A statement terminator ends the statement, so it is dropped together with the
	// whitespace around it: the caller appends its own FORMAT clause after the statement,
	// which a terminator left in place would put in a second statement. A terminator is
	// only dropped when the scan ended outside a quoted value, so one inside an
	// unterminated literal is not removed here.
	if quote == 0 {
		for len(out) > 0 && isBoundaryByte(out[len(out)-1]) {
			out = out[:len(out)-1]
		}
		return string(out)
	}

	return strings.TrimRight(string(out), " \t\r\n\v\f")
}

// commentEnd returns the length of the comment s starts with, or 0 when s does not start
// with one. A single line comment ends before its newline, so the newline itself is kept
// as a token separator, and an unterminated block comment runs to the end of the query.
func commentEnd(s string) int {
	if isLineCommentStart(s) {
		if end := strings.IndexByte(s, '\n'); end >= 0 {
			return end
		}
		return len(s)
	}
	if strings.HasPrefix(s, "/*") {
		if end := strings.Index(s[2:], "*/"); end >= 0 {
			return 2 + end + 2
		}
		return len(s)
	}
	return 0
}

// heredocEnd returns the length of the heredoc s starts with, or 0 when s does not start
// with one. A heredoc is written as $tag$value$tag$ with an optional tag, so its value
// may hold quote characters of its own without them opening a quoted value.
func heredocEnd(s string) int {
	if len(s) == 0 || s[0] != '$' {
		return 0
	}
	tag := 1
	for tag < len(s) && isWordByte(s[tag]) {
		tag++
	}
	if tag == len(s) || s[tag] != '$' {
		return 0
	}
	delimiter := s[:tag+1]
	end := strings.Index(s[len(delimiter):], delimiter)
	if end < 0 {
		return 0
	}
	return len(delimiter) + end + len(delimiter)
}

// appendCommentSeparator appends the space a removed comment leaves behind. It is only
// needed when the comment joined two tokens, so the whitespace of the statement is kept
// as written when the comment was already surrounded by some.
func appendCommentSeparator(out []byte, rest string) []byte {
	if len(out) > 0 && !isBoundaryByte(out[len(out)-1]) && rest != "" && !isBoundaryByte(rest[0]) {
		return append(out, ' ')
	}
	return out
}

const (
	valuesKeyword   = "VALUES"
	formatKeyword   = "FORMAT"
	settingsKeyword = "SETTINGS"
)

// isKeyword reports whether s starts with the given keyword. The keyword must follow a
// token boundary or the closing parenthesis of a column list, which ClickHouse also
// accepts as a separator ("INSERT INTO t (a, b)VALUES (1, 2)"), and must not be followed
// by a word byte, so an identifier that merely contains it is not mistaken for it. The
// preceding bytes are taken from the query sanitized so far rather than from the raw
// query, so a comment removed in front of the keyword still leaves a boundary behind it.
func isKeyword(s string, preceding []byte, keyword string) bool {
	if len(preceding) > 0 {
		if last := preceding[len(preceding)-1]; !isBoundaryByte(last) && last != ')' {
			return false
		}
	}
	if len(s) < len(keyword) || !strings.EqualFold(s[:len(keyword)], keyword) {
		return false
	}
	if len(s) > len(keyword) && isWordByte(s[len(keyword)]) {
		return false
	}
	return true
}

// followsInto reports whether the last word of the query sanitized so far is INTO, which
// makes the keyword that follows it the name of the table instead.
func followsInto(preceding []byte) bool {
	end := len(preceding)
	for end > 0 && isBoundaryByte(preceding[end-1]) {
		end--
	}
	start := end
	for start > 0 && isWordByte(preceding[start-1]) {
		start--
	}
	return strings.EqualFold(string(preceding[start:end]), "INTO")
}

// formatClauseEnd returns the length of the FORMAT clause s starts with, or 0 when s does
// not start with one. The keyword must be followed by the name of a format, so a trailing
// FORMAT keyword without one is left untouched.
func formatClauseEnd(s string, preceding []byte) int {
	if !isKeyword(s, preceding, formatKeyword) {
		return 0
	}
	i := len(formatKeyword)
	for i < len(s) && isBoundaryByte(s[i]) {
		i++
	}
	if i == len(formatKeyword) {
		return 0
	}
	name := i
	for i < len(s) && !isBoundaryByte(s[i]) {
		i++
	}
	if i == name {
		return 0
	}
	return i
}

// isBoundaryByte reports whether c ends a token: whitespace, or a statement terminator.
func isBoundaryByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\v' || c == '\f' || c == ';'
}

// isWordByte reports whether c can be part of an identifier. Bytes outside ASCII are
// treated as word bytes because they carry a multi byte character of one.
func isWordByte(c byte) bool {
	return c == '_' ||
		c >= 0x80 ||
		(c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z')
}

// isLineCommentStart reports whether s begins with a single line comment marker: "--",
// "#!" and "#" followed by whitespace. A "#" at the very end of the query also starts a
// comment, because the caller appends " FORMAT ..." after it.
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
