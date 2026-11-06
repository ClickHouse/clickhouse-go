package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	ErrInvalidValueInNamedDateValue = errors.New("invalid value in NamedDateValue for query parameter")
	ErrUnsupportedQueryParameter    = errors.New("unsupported query parameter type")

	hasQueryParamsRe = regexp.MustCompile("{.+:.+}")
	// queryParamDeclRe captures the name and declared type of each
	// {name:Type} placeholder. Types never contain braces, so a plain
	// non-brace run is enough and keeps the match cheap.
	queryParamDeclRe = regexp.MustCompile(`\{\s*(\w+)\s*:\s*([^{}]+?)\s*\}`)
)

// namedStringEscaper escapes control characters in a raw Go string sent as a
// top-level query parameter via Named. ClickHouse reads parameter values as
// TSV-escaped text (the server applies deserializeTextEscaped), so a raw tab,
// newline or CR acts as a field/record delimiter and is rejected, and a NUL is
// dropped. Escaping them here lets a caller pass ordinary Go strings and have
// control characters round-trip on both protocols:
//
//   - HTTP sends the value straight into the URL query string, and the server's
//     deserializeTextEscaped decodes these escapes once.
//   - TCP wraps the value in a quoted Field dump (encodeFieldDump → fieldDumpEscaper),
//     which re-escapes backslashes, so these escapes survive readQuoted and are
//     decoded by deserializeTextEscaped afterwards.
//
// Single quotes are intentionally left alone: fieldDumpEscaper handles them at the
// TCP boundary and URL-encoding covers them over HTTP.
var namedStringEscaper = strings.NewReplacer(
	`\`, `\\`, // backslash → \\: read as a literal backslash
	"\t", `\t`, // tab → \t
	"\n", `\n`, // newline → \n
	"\r", `\r`, // CR → \r
	"\x00", `\0`, // NUL → \0
)

func bindQueryOrAppendParameters(paramsProtocolSupport bool, options *QueryOptions, query string, timezone *time.Location, args ...any) (string, error) {
	// prefer native query parameters over legacy bind if query parameters provided explicit
	if len(options.parameters) > 0 {
		return query, nil
	}

	// validate if query contains a {<name>:<data type>} syntax, so it's intentional use of query parameters
	// parameter values will be loaded from `args ...any` for compatibility
	if paramsProtocolSupport &&
		len(args) > 0 &&
		hasQueryParamsRe.MatchString(query) {
		options.parameters = make(Parameters, len(args))
		paramTypes := declaredParamTypes(query)
		for _, a := range args {
			switch p := a.(type) {
			case driver.NamedValue:
				// A nil at the top level means SQL NULL, whose whole-text
				// marker is `\N`. The `NULL` keyword formatValue emits only
				// works nested inside arrays, maps, and tuples — at the top
				// level the server would read it as the string "NULL" (or
				// fail to parse it at all).
				if isNilParamValue(p.Value) {
					options.parameters[p.Name] = `\N`
					continue
				}
				// Strings, byte slices, and times at the top level are sent raw,
				// without quotes: the server reads a whole parameter value as-is,
				// and only quotes values nested inside arrays, maps, and
				// tuples. formatValue below applies the nested (quoted)
				// rules, so these skip it.
				switch v := p.Value.(type) {
				case string:
					options.parameters[p.Name] = namedStringEscaper.Replace(v)
					continue
				case *string:
					options.parameters[p.Name] = namedStringEscaper.Replace(*v)
					continue
				case []byte:
					if v == nil {
						options.parameters[p.Name] = `\N`
						continue
					}
					options.parameters[p.Name] = namedStringEscaper.Replace(string(v))
					continue
				case *[]byte:
					if *v == nil {
						options.parameters[p.Name] = `\N`
						continue
					}
					options.parameters[p.Name] = namedStringEscaper.Replace(string(*v))
					continue
				case time.Time:
					options.parameters[p.Name] = formatParamTime(timezone, paramTypes[p.Name], v)
					continue
				case *time.Time:
					options.parameters[p.Name] = formatParamTime(timezone, paramTypes[p.Name], *v)
					continue
				case time.Duration:
					options.parameters[p.Name] = formatDurationBody(v)
					continue
				case *time.Duration:
					options.parameters[p.Name] = formatDurationBody(*v)
					continue
				// Must follow the time.Time and time.Duration cases: their
				// String() output is Go's format, not the server's text format.
				case fmt.Stringer:
					options.parameters[p.Name] = v.String()
					continue
				}
				strVal, err := formatValueWithType(timezone, Seconds, p.Value, formatParamText, paramTypes[p.Name])
				if err != nil {
					return "", err
				}
				options.parameters[p.Name] = strVal

			case driver.NamedDateValue:
				if !p.Value.IsZero() && p.Name != "" {
					var formatted string
					if isDateParamType(paramTypes[p.Name]) {
						formatted = formatParamTime(timezone, paramTypes[p.Name], p.Value)
					} else {
						formatted = formatTimeWithScale(p.Value, TimeUnit(p.Scale))
					}
					options.parameters[p.Name] = formatted
					continue
				}
				return "", ErrInvalidValueInNamedDateValue

			default:
				return "", ErrUnsupportedQueryParameter
			}
		}

		return query, nil
	}

	return bind(timezone, query, args...)
}

// declaredParamTypes maps each {name:Type} placeholder in query to its
// declared type text. The declared type decides how a time.Time is rendered:
// Date and Date32 parameters need a calendar date, the DateTime family keeps
// the zone-free epoch form.
func declaredParamTypes(query string) map[string]string {
	matches := queryParamDeclRe.FindAllStringSubmatch(query, -1)
	types := make(map[string]string, len(matches))
	for _, m := range matches {
		types[m[1]] = m[2]
	}
	return types
}

// splitParamType splits a declared type like "Array(Date)" into its name and
// top-level arguments. A type without arguments returns the whole text as the
// name, so unknown declarations fall through unchanged.
func splitParamType(t string) (string, []string) {
	t = strings.TrimSpace(t)
	open := strings.IndexByte(t, '(')
	if open < 0 || !strings.HasSuffix(t, ")") {
		return t, nil
	}
	name := strings.TrimSpace(t[:open])
	inner := t[open+1 : len(t)-1]
	if strings.TrimSpace(inner) == "" {
		return name, nil
	}
	var args []string
	depth, start := 0, 0
	for i := 0; i < len(inner); i++ {
		switch inner[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(inner[start:i]))
				start = i + 1
			}
		}
	}
	return name, append(args, strings.TrimSpace(inner[start:]))
}

// unwrapParamType drops the wrappers that do not change how the value's text
// is written.
func unwrapParamType(t string) string {
	for {
		name, args := splitParamType(t)
		if (name == "Nullable" || name == "LowCardinality") && len(args) == 1 {
			t = args[0]
			continue
		}
		return t
	}
}

// paramElementType returns the element type declared for an Array, or "" when
// the declaration is not an array or is unknown.
func paramElementType(t string) string {
	name, args := splitParamType(unwrapParamType(t))
	if name == "Array" && len(args) == 1 {
		return args[0]
	}
	return ""
}

// paramMapTypes returns the key and value types declared for a Map, or empty
// strings when the declaration is not a map or is unknown.
func paramMapTypes(t string) (string, string) {
	name, args := splitParamType(unwrapParamType(t))
	if name == "Map" && len(args) == 2 {
		return args[0], args[1]
	}
	return "", ""
}

// isDateParamType reports whether t is Date or Date32: the two types whose
// text reader takes a calendar date and rejects epoch seconds.
func isDateParamType(t string) bool {
	name, args := splitParamType(unwrapParamType(t))
	return len(args) == 0 && (name == "Date" || name == "Date32")
}

// formatParamTime renders a time.Time for a server-side query parameter of
// the declared type. Date and Date32 take the calendar day as seen in the
// server's zone, matching what client-side binding produces; every other
// temporal type keeps the epoch form so the instant survives any timezone.
func formatParamTime(tz *time.Location, declType string, t time.Time) string {
	if isDateParamType(declType) {
		return t.In(tz).Format("2006-01-02")
	}
	return formatTimeParam(t)
}

// isNilParamValue reports whether v is nil itself or a typed nil pointer —
// the same values formatValue would render as NULL.
func isNilParamValue(v any) bool {
	if v == nil {
		return true
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		return rv.IsNil()
	}
	return false
}

// formatEpoch renders t as epoch seconds with exactly `digits` fractional
// digits (0, 3, 6 or 9), dropping anything finer.
//
// Times go out as epoch rather than wall-clock text because parameter text
// has no way to say which timezone the digits are in: the server would read
// a wall-clock string in the parameter's own zone, and whenever that differs
// from the value's zone the stored moment shifts by the offset. An epoch
// number means the same moment everywhere.
func formatEpoch(t time.Time, digits int) string {
	sec, ns := t.Unix(), int64(t.Nanosecond())
	if digits == 0 {
		return strconv.FormatInt(sec, 10)
	}
	pow := int64(1)
	for i := 0; i < digits; i++ {
		pow *= 10
	}
	frac := ns / (1_000_000_000 / pow)
	// Before 1970 the parts need care: Go gives -86400.5 as second -86401
	// plus 0.5 forward, and printing those two numbers as-is would read as
	// -86401.5. Put the sign in front of the combined value instead.
	sign := ""
	if sec < 0 {
		sign = "-"
		if frac > 0 {
			sec = -sec - 1
			frac = pow - frac
		} else {
			sec = -sec
		}
	}
	return fmt.Sprintf("%s%d.%0*d", sign, sec, digits, frac)
}

// formatTimeParam renders a time.Time sent through Named. Named carries no
// scale, so the fraction width comes from the value itself: whole seconds
// stay a bare integer, sub-second values keep their fraction. That way a
// DateTime64 parameter keeps the precision, and a plain DateTime parameter
// fails loudly on a fraction instead of silently losing it. DateNamed is
// the way to pick the width explicitly.
func formatTimeParam(t time.Time) string {
	switch ns := t.Nanosecond(); {
	case ns == 0:
		return formatEpoch(t, 0)
	case ns%1e6 == 0:
		return formatEpoch(t, 3)
	case ns%1e3 == 0:
		return formatEpoch(t, 6)
	default:
		return formatEpoch(t, 9)
	}
}

// formatTimeWithScale renders a time.Time sent through DateNamed: the
// caller's scale decides the fraction width (Seconds none, MilliSeconds 3
// digits, and so on), and anything finer is dropped.
func formatTimeWithScale(t time.Time, scale TimeUnit) string {
	switch scale {
	case MilliSeconds:
		return formatEpoch(t, 3)
	case MicroSeconds:
		return formatEpoch(t, 6)
	case NanoSeconds:
		return formatEpoch(t, 9)
	default:
		return formatEpoch(t, 0)
	}
}
