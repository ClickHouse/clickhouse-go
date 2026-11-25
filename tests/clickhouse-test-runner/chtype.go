package main

import (
	"fmt"
	"strings"
)

// chType is a parsed ClickHouse data type, for example
// `Array(Nullable(DateTime64(3, 'UTC')))`.
//
// Args holds the parameters inside the parentheses. A parameter is either a
// nested type (Type != nil) or a literal (Literal != ""), such as the `3` and
// `'UTC'` above or the `'a' = 1` of an `Enum8`. Name is set for the named
// elements of a `Tuple` or `Nested`, as in `Tuple(a Int32, b String)`.
type chType struct {
	Base string
	Args []chTypeArg
}

type chTypeArg struct {
	Name    string
	Type    *chType
	Literal string
}

func (t *chType) String() string {
	if len(t.Args) == 0 {
		return t.Base
	}
	parts := make([]string, len(t.Args))
	for i, a := range t.Args {
		switch {
		case a.Type != nil && a.Name != "":
			parts[i] = a.Name + " " + a.Type.String()
		case a.Type != nil:
			parts[i] = a.Type.String()
		default:
			parts[i] = a.Literal
		}
	}
	return t.Base + "(" + strings.Join(parts, ", ") + ")"
}

// typeArg returns the nested type of argument i, or nil.
func (t *chType) typeArg(i int) *chType {
	if i >= len(t.Args) {
		return nil
	}
	return t.Args[i].Type
}

// literalArg returns the literal of argument i without its single quotes, or
// "" when argument i is absent.
func (t *chType) literalArg(i int) string {
	if i >= len(t.Args) {
		return ""
	}
	return strings.Trim(t.Args[i].Literal, "'")
}

// parseCHType parses the type name that the server sends in a block header.
func parseCHType(s string) (*chType, error) {
	t, rest, err := parseTypeAt(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("parse type %q: %w", s, err)
	}
	if strings.TrimSpace(rest) != "" {
		return nil, fmt.Errorf("parse type %q: unexpected trailing text %q", s, rest)
	}
	return t, nil
}

func parseTypeAt(s string) (*chType, string, error) {
	i := 0
	for i < len(s) && (isWordByte(s[i]) || s[i] == '.') {
		i++
	}
	if i == 0 {
		return nil, s, fmt.Errorf("expected a type name at %q", s)
	}
	t := &chType{Base: s[:i]}
	rest := strings.TrimLeft(s[i:], " ")
	if !strings.HasPrefix(rest, "(") {
		return t, rest, nil
	}
	parts, rest, err := splitTypeArgs(rest)
	if err != nil {
		return nil, s, err
	}
	for _, p := range parts {
		t.Args = append(t.Args, parseTypeArg(t.Base, p))
	}
	return t, rest, nil
}

// parseTypeArg turns one raw parameter into a nested type, a named nested
// type, or a literal.
func parseTypeArg(base, raw string) chTypeArg {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw[0] == '\'' || raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9') {
		return chTypeArg{Literal: raw}
	}
	switch base {
	case "Enum8", "Enum16", "Enum":
		return chTypeArg{Literal: raw}
	case "AggregateFunction", "SimpleAggregateFunction":
		// The first argument is a function name, such as `sum` or
		// `quantiles(0.5, 0.9)`. It is not a type.
		if t, rest, err := parseTypeAt(raw); err == nil && strings.TrimSpace(rest) == "" && isKnownTypeName(t.Base) {
			return chTypeArg{Type: t}
		}
		return chTypeArg{Literal: raw}
	case "Tuple", "Nested", "JSON", "Object", "Variant":
		// Named element: `name Type` (the name can be quoted with backticks).
		if name, typ, ok := splitNamedElement(raw); ok {
			if t, rest, err := parseTypeAt(typ); err == nil && strings.TrimSpace(rest) == "" {
				return chTypeArg{Name: name, Type: t}
			}
		}
	}
	if t, rest, err := parseTypeAt(raw); err == nil && strings.TrimSpace(rest) == "" {
		return chTypeArg{Type: t}
	}
	return chTypeArg{Literal: raw}
}

// splitNamedElement splits `name Type` into its two parts. It returns ok=false
// for an element that is only a type, such as `Int32` or `Array(String)`.
func splitNamedElement(raw string) (name, typ string, ok bool) {
	if strings.HasPrefix(raw, "`") {
		end := strings.Index(raw[1:], "`")
		if end < 0 {
			return "", "", false
		}
		return raw[1 : end+1], strings.TrimSpace(raw[end+2:]), true
	}
	i := 0
	for i < len(raw) && (isWordByte(raw[i]) || raw[i] == '.') {
		i++
	}
	if i == 0 || i >= len(raw) || raw[i] != ' ' {
		return "", "", false
	}
	return raw[:i], strings.TrimSpace(raw[i:]), true
}

// splitTypeArgs splits `(a, b(c, d), 'e,f')rest` into its top-level
// parameters and the text after the closing parenthesis.
func splitTypeArgs(s string) ([]string, string, error) {
	var (
		parts []string
		depth int
		start = 1
		quote byte
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '`', '"':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if p := strings.TrimSpace(s[start:i]); p != "" || len(parts) > 0 {
					parts = append(parts, p)
				}
				return parts, s[i+1:], nil
			}
		case ',':
			if depth == 1 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return nil, s, fmt.Errorf("unbalanced parentheses in %q", s)
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// isKnownTypeName is used only to tell `SimpleAggregateFunction(sum, UInt64)`
// (function name, then type) apart; a false negative only makes the renderer
// report an unsupported type.
func isKnownTypeName(name string) bool {
	switch name {
	case "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
		"UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
		"Float32", "Float64", "BFloat16", "Bool", "String", "FixedString",
		"Date", "Date32", "DateTime", "DateTime64", "Time", "Time64", "UUID", "IPv4", "IPv6",
		"Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
		"Enum8", "Enum16", "Nullable", "LowCardinality", "Array", "Tuple", "Map", "Nothing",
		"Point", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon":
		return true
	}
	return false
}
