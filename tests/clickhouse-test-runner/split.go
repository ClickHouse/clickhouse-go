package main

import (
	"strings"
)

// statement is one query of a multi-query script, split the way
// `clickhouse-client` splits it (see `ClientBase::executeMultiQuery`).
type statement struct {
	// SQL is the text to send to the server: from the first non-whitespace
	// character (leading comments included) to the end of the query, with
	// the semicolon.
	SQL string
	// Full is SQL plus the semicolon and the same-line trailing comment. The
	// client reads the test hints from this text and prints it for `echo`.
	Full string
	Hint testHint
}

// splitStatements splits a script into statements.
//
// It follows `clickhouse-client`:
//   - The `-- Tags:` header (the first line and the `--` lines after it) is
//     not part of any statement.
//   - A statement ends at a top-level `;`. A comment on the same line after
//     the `;` belongs to that statement; this is where test hints are.
//   - The inline data of `INSERT ... FORMAT <name>` (other than `Values`)
//     ends at an empty line, not at `;`.
//   - Comment-only text after the last statement is not a statement.
func splitStatements(text string) []statement {
	var (
		out  []statement
		pos  = testTagsLength(text)
		echo bool
	)
	for {
		pos = skipWhitespace(text, pos)
		if pos >= len(text) || !hasSignificantToken(text[pos:]) {
			return out
		}
		begin := pos
		end := scanStatementEnd(text, pos)
		sqlEnd := end
		// Consume the semicolons after the statement.
		pos = end
		for pos < len(text) && text[pos] == ';' {
			pos++
			if next := skipWhitespaceAndComments(text, pos); next < len(text) && text[next] == ';' {
				pos = next
			}
		}
		// Add the same-line trailing comment when the next statement starts
		// after the end of this line.
		newline := strings.IndexByte(text[pos:], '\n')
		if newline < 0 {
			newline = len(text)
		} else {
			newline += pos
		}
		if newline <= skipWhitespaceAndComments(text, pos) {
			pos = newline
		}
		// clickhouse-client sends the statement with its semicolon. Tests
		// that read `system.query_log` see it.
		sql := strings.TrimRight(text[begin:sqlEnd], " \t\r\n")
		if sqlEnd < len(text) && text[sqlEnd] == ';' {
			sql = text[begin : sqlEnd+1]
		}
		st := statement{
			SQL:  sql,
			Full: strings.TrimRight(text[begin:pos], " \t\r\n"),
		}
		if sqlEnd == len(text) {
			// The last statement has no semicolon: clickhouse-client echoes
			// all the rest of the text, with its trailing newlines.
			st.Full = text[begin:]
		}
		st.Hint = parseTestHint(st.Full)
		if st.Hint.Echo != nil {
			echo = *st.Hint.Echo
		}
		st.Hint.echoThis = echo
		out = append(out, st)
	}
}

// testTagsLength mirrors `getTestTagsLength`: the length of the leading
// `-- Tags: ...` line and the `--` comment lines right after it.
func testTagsLength(text string) int {
	pos := 0
	first := true
	for {
		lineStart := pos
		for pos < len(text) && (text[pos] == ' ' || text[pos] == '\t') {
			pos++
		}
		if !strings.HasPrefix(text[pos:], "--") {
			return lineStart
		}
		pos += 2
		if first {
			for pos < len(text) && (text[pos] == ' ' || text[pos] == '\t') {
				pos++
			}
			if !strings.HasPrefix(text[pos:], "Tags:") {
				return 0
			}
			pos += len("Tags:")
			first = false
		}
		eol := strings.IndexAny(text[pos:], "\r\n")
		if eol < 0 {
			return len(text)
		}
		eol += pos
		eolLen := 1
		if eol+1 < len(text) && (text[eol+1] == '\r' || text[eol+1] == '\n') && text[eol+1] != text[eol] {
			eolLen = 2
		}
		pos = eol + eolLen
	}
}

// scanStatementEnd returns the offset of the `;` that ends the statement at
// start, or len(text).
func scanStatementEnd(text string, start int) int {
	parens := 0
	for i := start; i < len(text); {
		c := text[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(text, i)
		case c == '-' && strings.HasPrefix(text[i:], "--"):
			i = skipLineComment(text, i)
		case c == '#' && (strings.HasPrefix(text[i:], "#!") || strings.HasPrefix(text[i:], "# ")):
			i = skipLineComment(text, i)
		case c == '/' && strings.HasPrefix(text[i:], "/*"):
			i = skipBlockComment(text, i)
		case c == '$':
			if end := heredocEnd(text, i); end > 0 {
				i = end
			} else {
				i++
			}
		case c == '(' || c == '[':
			parens++
			i++
		case c == ')' || c == ']':
			parens--
			i++
		case c == ';' && parens <= 0:
			return i
		case isWordByte(c):
			j := i
			for j < len(text) && isWordByte(text[j]) {
				j++
			}
			if strings.EqualFold(text[i:j], "FORMAT") && parens <= 0 {
				if dataStart, ok := insertDataStart(text, start, j); ok {
					// The inline data ends at an empty line.
					if k := strings.Index(text[dataStart:], "\n\n"); k >= 0 {
						return dataStart + k
					}
					return len(text)
				}
			}
			i = j
		default:
			i++
		}
	}
	return len(text)
}

// insertDataStart reports whether the `FORMAT` keyword that ends at
// formatEnd starts the inline data of an INSERT statement that begins at
// start. It returns the offset of the data after the format name. The
// `Values` format is excluded: its data ends at `;`, like a query.
func insertDataStart(text string, start, formatEnd int) (int, bool) {
	if leadingKeyword(text[start:formatEnd]) != "INSERT" {
		return 0, false
	}
	i := formatEnd
	if i >= len(text) || (text[i] != ' ' && text[i] != '\t' && text[i] != '\n' && text[i] != '\r') {
		return 0, false
	}
	for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r') {
		i++
	}
	j := i
	for j < len(text) && isWordByte(text[j]) {
		j++
	}
	if j == i || strings.EqualFold(text[i:j], "Values") || !isFormatName(text[i:j]) {
		return 0, false
	}
	// After a SELECT, the FORMAT clause comes at the end of the SELECT, so
	// after its top-level FROM: `INSERT INTO t SELECT * FROM input('x UInt8')
	// FORMAT TSV`. Before that FROM, `format` is in the select list
	// (`SELECT format x FROM s`, `SELECT format DIV 2 FROM s`).
	if !formatAfterSelectFrom(text[start:formatEnd-len("FORMAT")]) || startsExpression(text[start:], formatEnd-len("FORMAT")-start) {
		return 0, false
	}
	return j, true
}

// expressionKeywords are words after which a word starts an expression or a
// table reference, so a `format` after them is an identifier
// (`SELECT format x`, `WHERE format = 1`, `FROM format Null`,
// `INSERT INTO format SELECT ...`), not the FORMAT clause. The FORMAT
// clause never comes right after FROM, JOIN or INTO, so an alias that is a
// format name (`FROM format TSV`) does not count.
var expressionKeywords = map[string]bool{
	"SELECT": true, "DISTINCT": true, "BY": true, "WHERE": true, "PREWHERE": true,
	"HAVING": true, "QUALIFY": true, "AND": true, "OR": true, "NOT": true, "ON": true,
	"USING": true, "WHEN": true, "THEN": true, "ELSE": true, "CASE": true, "AS": true,
	"IN": true, "LIKE": true, "ILIKE": true, "BETWEEN": true, "IS": true, "DIV": true,
	"MOD": true, "TOP": true, "INTERVAL": true,
	"FROM": true, "JOIN": true, "INTO": true,
}

// startsExpression tells if the word at offset i of sql starts an
// expression: the last significant token before it (comments do not count)
// is a comma, an opening parenthesis, an operator, or a word of
// expressionKeywords. Then a `format` at i is an identifier, as in
// `SELECT 1 AS format, format x`. A FORMAT clause follows a complete query,
// which ends with an identifier, a literal, `)` or a select-star
// (`SELECT * FORMAT TSV`).
func startsExpression(sql string, i int) bool {
	toks := significantTokens(sql[:i])
	if len(toks) == 0 {
		return true
	}
	last := toks[len(toks)-1]
	if last == "*" {
		// A star after SELECT, DISTINCT, `,`, `(` or `.` (t.*) is a
		// select-star, which can end a query; otherwise it multiplies.
		if len(toks) == 1 {
			return true
		}
		switch prev := strings.ToUpper(toks[len(toks)-2]); prev {
		case "SELECT", "DISTINCT", ",", "(", ".":
			return false
		}
		return true
	}
	if len(last) == 1 && strings.Contains(",([=<>+-/%|&!^~?:", last) {
		return true
	}
	if !expressionKeywords[strings.ToUpper(last)] {
		return false
	}
	// A keyword right after AS or `.` is a name: an alias
	// (`SELECT 1 AS into FORMAT TSV`) or a column (`t.from`). The query is
	// then complete.
	if len(toks) >= 2 {
		if prev := strings.ToUpper(toks[len(toks)-2]); prev == "AS" || prev == "." {
			return false
		}
	}
	return true
}

// significantTokens returns the tokens of sql outside comments: words,
// quoted strings and identifiers (as the quote character), heredocs (as `'`:
// a heredoc is a string literal), and single punctuation characters.
func significantTokens(sql string) []string {
	var toks []string
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case isSpace(c):
			i++
		case strings.HasPrefix(sql[i:], "--"),
			strings.HasPrefix(sql[i:], "#!"), strings.HasPrefix(sql[i:], "# "):
			i = skipLineComment(sql, i)
		case strings.HasPrefix(sql[i:], "/*"):
			i = skipBlockComment(sql, i)
		case c == '\'' || c == '"' || c == '`':
			toks = append(toks, string(c))
			i = skipQuoted(sql, i)
		case c == '$':
			if e := heredocEnd(sql, i); e > 0 {
				toks = append(toks, "'")
				i = e
			} else {
				toks = append(toks, "$")
				i++
			}
		case isWordByte(c):
			j := i
			for j < len(sql) && isWordByte(sql[j]) {
				j++
			}
			toks = append(toks, sql[i:j])
			i = j
		default:
			toks = append(toks, string(c))
			i++
		}
	}
	return toks
}

// formatAfterSelectFrom tells if a FORMAT that follows head can be the
// FORMAT clause of an INSERT: head has no top-level SELECT, or a top-level
// FROM follows its last top-level SELECT.
func formatAfterSelectFrom(head string) bool {
	afterSelect, sawFrom := false, false
	for _, w := range topLevelWords(head) {
		switch strings.ToUpper(w) {
		case "SELECT":
			afterSelect, sawFrom = true, false
		case "FROM":
			sawFrom = true
		}
	}
	return !afterSelect || sawFrom
}

// topLevelWords returns the words of sql outside quotes, heredocs, comments
// and parentheses.
func topLevelWords(sql string) []string {
	var words []string
	depth := 0
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(sql, i)
		case c == '$':
			if e := heredocEnd(sql, i); e > 0 {
				i = e
			} else {
				i++
			}
		case strings.HasPrefix(sql[i:], "--"),
			strings.HasPrefix(sql[i:], "#!"), strings.HasPrefix(sql[i:], "# "):
			i = skipLineComment(sql, i)
		case strings.HasPrefix(sql[i:], "/*"):
			i = skipBlockComment(sql, i)
		case c == '(' || c == '[':
			depth++
			i++
		case c == ')' || c == ']':
			depth--
			i++
		case isWordByte(c):
			j := i
			for j < len(sql) && isWordByte(sql[j]) {
				j++
			}
			if depth == 0 {
				words = append(words, sql[i:j])
			}
			i = j
		default:
			i++
		}
	}
	return words
}

// isFormatName tells if word is the name of a format of the pinned server
// (formats.go, from system.formats). After the word FORMAT, any other word
// shows that `format` is an identifier: a column (`SELECT format x`), a
// table (`INSERT INTO format SELECT ...`, `FROM format src`), or a sort key
// (`ORDER BY format DESC`).
func isFormatName(word string) bool {
	return formatNames[strings.ToLower(word)]
}

func skipQuoted(text string, i int) int {
	q := text[i]
	for j := i + 1; j < len(text); j++ {
		switch text[j] {
		case '\\':
			j++
		case q:
			// A doubled quote is an escaped quote.
			if j+1 < len(text) && text[j+1] == q {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(text)
}

func skipLineComment(text string, i int) int {
	if nl := strings.IndexByte(text[i:], '\n'); nl >= 0 {
		return i + nl
	}
	return len(text)
}

func skipBlockComment(text string, i int) int {
	// Block comments nest in ClickHouse.
	depth := 0
	for j := i; j < len(text)-1; {
		switch {
		case text[j] == '/' && text[j+1] == '*':
			depth++
			j += 2
		case text[j] == '*' && text[j+1] == '/':
			depth--
			j += 2
			if depth == 0 {
				return j
			}
		default:
			j++
		}
	}
	return len(text)
}

// heredocEnd returns the end offset of the `$tag$ ... $tag$` heredoc that
// starts at i, or 0 when the `$` at i does not start one.
func heredocEnd(text string, i int) int {
	tagEnd := strings.IndexByte(text[i+1:], '$')
	if tagEnd < 0 {
		return 0
	}
	tagEnd += i + 1
	for k := i + 1; k < tagEnd; k++ {
		if !isWordByte(text[k]) {
			return 0
		}
	}
	delim := text[i : tagEnd+1]
	end := strings.Index(text[tagEnd+1:], delim)
	if end < 0 {
		return 0
	}
	return tagEnd + 1 + end + len(delim)
}

func skipWhitespace(text string, i int) int {
	for i < len(text) && isSpace(text[i]) {
		i++
	}
	return i
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

func skipWhitespaceAndComments(text string, i int) int {
	for i < len(text) {
		switch {
		case isSpace(text[i]):
			i++
		case strings.HasPrefix(text[i:], "--"):
			i = skipLineComment(text, i)
		case strings.HasPrefix(text[i:], "#!") || strings.HasPrefix(text[i:], "# "):
			i = skipLineComment(text, i)
		case strings.HasPrefix(text[i:], "/*"):
			i = skipBlockComment(text, i)
		default:
			return i
		}
	}
	return i
}

func hasSignificantToken(text string) bool {
	return skipWhitespaceAndComments(text, 0) < len(text)
}

// leadingKeyword returns the first word of a statement in upper case,
// after comments, whitespace and opening parentheses.
func leadingKeyword(sql string) string {
	i := skipWhitespaceAndComments(sql, 0)
	for i < len(sql) && (sql[i] == '(' || isSpace(sql[i])) {
		i = skipWhitespaceAndComments(sql, i+1)
	}
	j := i
	for j < len(sql) && isWordByte(sql[j]) {
		j++
	}
	return strings.ToUpper(sql[i:j])
}
