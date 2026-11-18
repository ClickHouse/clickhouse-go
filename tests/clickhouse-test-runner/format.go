package main

import (
	"strings"
)

// outputFormat is how the shim writes a result set.
type outputFormat struct {
	Name      string
	Names     bool // a header line with the column names
	Types     bool // a header line with the column types
	Raw       bool // top-level strings are not escaped
	Discard   bool // the `Null` format
	Supported bool
}

// resultKeywords are the leading keywords of the statements that return a
// result set. The shim runs these with Conn.Query and the other statements
// with Conn.Exec.
var resultKeywords = map[string]bool{
	"SELECT":   true,
	"WITH":     true,
	"SHOW":     true,
	"DESC":     true,
	"DESCRIBE": true,
	"EXISTS":   true,
	"EXPLAIN":  true,
	"CHECK":    true,
	// FROM-first syntax: FROM t SELECT x, and pipe syntax: FROM t |> ...
	"FROM": true,
}

func returnsRows(sql string) bool {
	return resultKeywords[leadingKeyword(sql)]
}

// splitOutputFormat removes a top-level `FORMAT <name>` clause from a
// result-set statement and returns the statement and the format.
//
// clickhouse-go always reads the result as `Native` and the shim writes it
// as `TabSeparated`, so only the `TabSeparated` family and `Null` can be
// written. A statement with another format fails.
func splitOutputFormat(sql string) (string, outputFormat) {
	start, end, name := findFormatClause(sql)
	if start < 0 {
		return sql, outputFormat{Name: "TabSeparated", Supported: true}
	}
	f := outputFormat{Name: name}
	// Format names are case-insensitive.
	switch strings.ToLower(name) {
	case "tabseparated", "tsv":
		f.Supported = true
	case "tabseparatedwithnames", "tsvwithnames":
		f.Supported, f.Names = true, true
	case "tabseparatedwithnamesandtypes", "tsvwithnamesandtypes":
		f.Supported, f.Names, f.Types = true, true, true
	case "tabseparatedraw", "tsvraw", "raw":
		f.Supported, f.Raw = true, true
	case "tabseparatedrawwithnames", "tsvrawwithnames", "rawwithnames":
		f.Supported, f.Raw, f.Names = true, true, true
	case "tabseparatedrawwithnamesandtypes", "tsvrawwithnamesandtypes", "rawwithnamesandtypes":
		f.Supported, f.Raw, f.Names, f.Types = true, true, true, true
	case "null":
		f.Supported, f.Discard = true, true
	}
	return sql[:start] + sql[end:], f
}

// findFormatClause returns the position of a top-level `FORMAT <name>`
// clause, or start=-1.
func findFormatClause(sql string) (start, end int, name string) {
	depth := 0
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(sql, i)
		case strings.HasPrefix(sql[i:], "--"),
			strings.HasPrefix(sql[i:], "#!"), strings.HasPrefix(sql[i:], "# "):
			i = skipLineComment(sql, i)
		case strings.HasPrefix(sql[i:], "/*"):
			i = skipBlockComment(sql, i)
		case c == '$':
			if e := heredocEnd(sql, i); e > 0 {
				i = e
			} else {
				i++
			}
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
			if depth == 0 && strings.EqualFold(sql[i:j], "FORMAT") && (i == 0 || sql[i-1] != '.') {
				k := skipWhitespaceAndComments(sql, j)
				if k > j && k < len(sql) && (sql[k] == '`' || sql[k] == '"') {
					// A quoted format name: FORMAT `Null`.
					m := skipQuoted(sql, k)
					if name, ok := unquoteIdentifier(sql[k:m]); ok {
						return i, m, name
					}
				}
				m := k
				for m < len(sql) && isWordByte(sql[m]) {
					m++
				}
				if m > k && k > j {
					return i, m, sql[k:m]
				}
			}
			i = j
		default:
			i++
		}
	}
	return -1, -1, ""
}
