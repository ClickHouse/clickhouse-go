package clickhouse

import (
	std_driver "database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	ErrInvalidTimezone = errors.New("invalid timezone value")
)

func Named(name string, value any) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

type TimeUnit uint8

const (
	Seconds TimeUnit = iota
	MilliSeconds
	MicroSeconds
	NanoSeconds
)

type GroupSet struct {
	Value []any
}

type ArraySet []any

func DateNamed(name string, value time.Time, scale TimeUnit) driver.NamedDateValue {
	return driver.NamedDateValue{
		Name:  name,
		Value: value,
		Scale: uint8(scale),
	}
}

func bind(tz *time.Location, query string, args ...any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var (
		haveNumeric    bool
		havePositional bool
	)

	allArgumentsNamed, err := checkAllNamedArguments(args...)
	if err != nil {
		return "", err
	}

	if allArgumentsNamed {
		return bindNamed(tz, query, args...)
	}

	haveNumeric, havePositional = bindParamsFormats(query)
	if haveNumeric && havePositional {
		return "", ErrBindMixedParamsFormats
	}
	if haveNumeric {
		return bindNumeric(tz, query, args...)
	}
	return bindPositional(tz, query, args...)
}

func checkAllNamedArguments(args ...any) (bool, error) {
	var (
		haveNamed     bool
		haveAnonymous bool
	)
	for _, v := range args {
		switch v.(type) {
		case driver.NamedValue, driver.NamedDateValue:
			haveNamed = true
		default:
			haveAnonymous = true
		}
		if haveNamed && haveAnonymous {
			return haveNamed, ErrBindMixedParamsFormats
		}
	}
	return haveNamed, nil
}

// bindQuoteState tracks whether the scanner is currently inside a region of the
// query where bind placeholders ('?', '$N', '@name') must NOT be substituted: a
// quoted identifier (backtick or double quote), a string literal (single quote),
// or a comment.
//
// ClickHouse comment syntax: single-line comments start with "--", "#" or "#!"
// and run to the end of the line; block comments are delimited by "/*" and "*/"
// and may be nested.
type bindQuoteState struct {
	inBacktick    bool
	inSingle      bool
	inDouble      bool
	inLineComment bool
	blockComment  int // nesting depth of /* */ comments (ClickHouse nests them)
}

// inProtectedContext reports whether the current position is inside a quoted
// identifier, string literal, or comment: any region where '?', '$N' and
// '@name' markers are part of the query text rather than bind placeholders.
func (s *bindQuoteState) inProtectedContext() bool {
	return s.inBacktick || s.inSingle || s.inDouble || s.inLineComment || s.blockComment > 0
}

// inIdentifierOrComment reports whether the current position is inside a quoted
// identifier (backtick or double quote) or a comment. In these contexts the
// query text is passed through untouched, including any backslash that precedes
// a '?'. This is deliberately distinct from a single-quoted string literal,
// where a "\?" is unescaped to a literal "?" for backward compatibility (see
// bindPositional).
func (s *bindQuoteState) inIdentifierOrComment() bool {
	return s.inBacktick || s.inDouble || s.inLineComment || s.blockComment > 0
}

// update consumes the byte at pos and advances the quote/comment state. It
// returns the index of the last byte it consumed, which may be pos+1 when a
// two-byte token (a doubled quote delimiter, "--", "/*" or "*/") is recognized
// so the caller's loop skips the second byte. Doubled delimiters and backslash
// escapes keep the scanner inside the current quoted context.
func (s *bindQuoteState) update(query string, pos int) int {
	switch {
	case s.inLineComment:
		if query[pos] == '\n' {
			s.inLineComment = false
		}
	case s.blockComment > 0:
		// Block comments nest in ClickHouse, so track depth rather than a bool.
		switch {
		case query[pos] == '/' && pos+1 < len(query) && query[pos+1] == '*':
			s.blockComment++
			return pos + 1
		case query[pos] == '*' && pos+1 < len(query) && query[pos+1] == '/':
			s.blockComment--
			return pos + 1
		}
	case s.inBacktick:
		if query[pos] == '`' && !isEscaped(query, pos) {
			if pos+1 < len(query) && query[pos+1] == '`' {
				return pos + 1
			}
			s.inBacktick = false
		}
	case s.inSingle:
		if query[pos] == '\'' && !isEscaped(query, pos) {
			if pos+1 < len(query) && query[pos+1] == '\'' {
				return pos + 1
			}
			s.inSingle = false
		}
	case s.inDouble:
		if query[pos] == '"' && !isEscaped(query, pos) {
			if pos+1 < len(query) && query[pos+1] == '"' {
				return pos + 1
			}
			s.inDouble = false
		}
	default:
		// Raw context: a backslash-escaped delimiter does not open anything.
		if isEscaped(query, pos) {
			return pos
		}
		switch {
		case query[pos] == '`':
			s.inBacktick = true
		case query[pos] == '\'':
			s.inSingle = true
		case query[pos] == '"':
			s.inDouble = true
		case query[pos] == '#':
			// "#" and "#!" both start a single-line comment.
			s.inLineComment = true
		case query[pos] == '-' && pos+1 < len(query) && query[pos+1] == '-':
			s.inLineComment = true
			return pos + 1
		case query[pos] == '/' && pos+1 < len(query) && query[pos+1] == '*':
			s.blockComment++
			return pos + 1
		}
	}
	return pos
}

func isEscaped(query string, pos int) bool {
	backslashes := 0
	for i := pos - 1; i >= 0 && query[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// isNameChar reports whether ch is valid in a named placeholder (@name); it
// mirrors the previous bindNamedRe pattern `@[a-zA-Z0-9_]+`.
func isNameChar(ch byte) bool {
	return ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}

func bindParamsFormats(query string) (haveNumeric, havePositional bool) {
	var state bindQuoteState
	for i := 0; i < len(query); i++ {
		if !state.inProtectedContext() {
			switch {
			case query[i] == '?' && (i == 0 || query[i-1] != '\\'):
				havePositional = true
			case query[i] == '$' && i+1 < len(query) && isDigit(query[i+1]):
				haveNumeric = true
			}
			if haveNumeric && havePositional {
				return haveNumeric, havePositional
			}
		}
		i = state.update(query, i)
	}
	return haveNumeric, havePositional
}

func bindPositional(tz *time.Location, query string, args ...any) (_ string, err error) {
	var (
		lastMatchIndex = -1 // Position of previous match for copying
		argIndex       = 0  // Index for the argument at current position
		buf            = make([]byte, 0, len(query))
		unbindCount    = 0 // Number of positional arguments that couldn't be matched
		state          bindQuoteState
	)

	for i := 0; i < len(query); i++ {
		// It's fine looping through the query string as bytes, because the (fixed) characters we're looking for
		// are in the ASCII range to won't take up more than one byte.
		if query[i] == '?' {
			// Inside identifier quotes or comments the text is passed through
			// unchanged, including any backslash that precedes the '?'.
			if state.inIdentifierOrComment() {
				continue
			}
			if i > 0 && query[i-1] == '\\' {
				// Escaped "\?" becomes a literal "?" (the backslash is dropped).
				// Applies in raw and single-quoted contexts; kept for backward
				// compatibility.
				buf = append(buf, query[lastMatchIndex+1:i-1]...)
				buf = append(buf, '?')
				lastMatchIndex = i
				continue
			}
			if state.inSingle {
				// An unescaped '?' inside a string literal is verbatim.
				continue
			}

			// Copy all previous index to here characters
			buf = append(buf, query[lastMatchIndex+1:i]...)

			// Append the argument value
			if argIndex < len(args) {
				v := args[argIndex]
				if fn, ok := v.(std_driver.Valuer); ok {
					if v, err = fn.Value(); err != nil {
						return "", err
					}
				}

				value, err := format(tz, Seconds, v)
				if err != nil {
					return "", err
				}

				buf = append(buf, value...)
				argIndex++
			} else {
				unbindCount++
			}

			lastMatchIndex = i
			continue
		}
		i = state.update(query, i)
	}

	// If there were no replacements, quick return without copying the string
	if lastMatchIndex < 0 {
		return query, nil
	}

	// Append the remainder
	buf = append(buf, query[lastMatchIndex+1:]...)

	if unbindCount > 0 {
		return "", fmt.Errorf("have no arg for param ? at last %d positions", unbindCount)
	}

	return string(buf), nil
}

func bindNumeric(tz *time.Location, query string, args ...any) (_ string, err error) {
	var (
		lastMatchIndex = -1
		unbind         = make(map[string]struct{})
		params         = make(map[string]string)
		buf            = make([]byte, 0, len(query))
		state          bindQuoteState
	)
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", err
			}
		}
		val, err := format(tz, Seconds, v)
		if err != nil {
			return "", err
		}
		params[fmt.Sprintf("$%d", i+1)] = val
	}

	for i := 0; i < len(query); i++ {
		if !state.inProtectedContext() && query[i] == '$' && i+1 < len(query) && isDigit(query[i+1]) {
			j := i + 2
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			param := query[i:j]
			buf = append(buf, query[lastMatchIndex+1:i]...)
			if value, found := params[param]; found {
				buf = append(buf, value...)
			} else {
				unbind[param] = struct{}{}
			}
			lastMatchIndex = j - 1
			i = j - 1
			continue
		}
		i = state.update(query, i)
	}
	if lastMatchIndex < 0 {
		return query, nil
	}
	buf = append(buf, query[lastMatchIndex+1:]...)
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %s param", param)
	}
	return string(buf), nil
}

func bindNamed(tz *time.Location, query string, args ...any) (_ string, err error) {
	var (
		lastMatchIndex = -1
		unbind         = make(map[string]struct{})
		params         = make(map[string]string)
		buf            = make([]byte, 0, len(query))
		state          bindQuoteState
	)
	for _, v := range args {
		switch v := v.(type) {
		case driver.NamedValue:
			value := v.Value
			if fn, ok := v.Value.(std_driver.Valuer); ok {
				if value, err = fn.Value(); err != nil {
					return "", err
				}
			}
			val, err := format(tz, Seconds, value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		case driver.NamedDateValue:
			val, err := format(tz, TimeUnit(v.Scale), v.Value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		}
	}

	for i := 0; i < len(query); i++ {
		// A named placeholder is "@" followed by at least one name character, and
		// only counts outside of quoted identifiers, string literals and comments.
		if !state.inProtectedContext() && query[i] == '@' && i+1 < len(query) && isNameChar(query[i+1]) {
			j := i + 1
			for j < len(query) && isNameChar(query[j]) {
				j++
			}
			param := query[i:j]
			buf = append(buf, query[lastMatchIndex+1:i]...)
			if value, found := params[param]; found {
				buf = append(buf, value...)
			} else {
				unbind[param] = struct{}{}
			}
			lastMatchIndex = j - 1
			i = j - 1
			continue
		}
		i = state.update(query, i)
	}

	// If there were no replacements, quick return without copying the string.
	if lastMatchIndex < 0 {
		return query, nil
	}
	buf = append(buf, query[lastMatchIndex+1:]...)
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %q param", param)
	}
	return string(buf), nil
}

func formatTime(tz *time.Location, scale TimeUnit, value time.Time) (string, error) {
	locVal := value.Location().String()

	switch locVal {
	case "Local", "":
		// It's required to pass timestamp as string due to decimal overflow for higher precision,
		// but zero-value string "toDateTime('0')" will be not parsed by ClickHouse.
		if value.Unix() == 0 {
			return "toDateTime(0)", nil
		}

		switch scale {
		case Seconds:
			return fmt.Sprintf("toDateTime('%d')", value.Unix()), nil
		case MilliSeconds:
			return fmt.Sprintf("toDateTime64('%d', 3)", value.UnixMilli()), nil
		case MicroSeconds:
			return fmt.Sprintf("toDateTime64('%d', 6)", value.UnixMicro()), nil
		case NanoSeconds:
			return fmt.Sprintf("toDateTime64('%d', 9)", value.UnixNano()), nil
		}
	case tz.String():
		if scale == Seconds {
			return value.Format("toDateTime('2006-01-02 15:04:05')"), nil
		}
		return fmt.Sprintf("toDateTime64('%s', %d)", value.Format(fmt.Sprintf("2006-01-02 15:04:05.%0*d", int(scale*3), 0)), int(scale*3)), nil
	}

	// Escape the timezone string (timezone may contain malicious SQL query)
	escapedTimezone := stringQuoteReplacer.Replace(locVal)
	if locVal != escapedTimezone {
		return "", fmt.Errorf("%w: %q", ErrInvalidTimezone, locVal)
	}

	if scale == Seconds {
		return fmt.Sprintf("toDateTime('%s', '%s')", value.Format("2006-01-02 15:04:05"), escapedTimezone), nil
	}
	return fmt.Sprintf("toDateTime64('%s', %d, '%s')", value.Format(fmt.Sprintf("2006-01-02 15:04:05.%0*d", int(scale*3), 0)), int(scale*3), escapedTimezone), nil
}

var stringQuoteReplacer = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// format turns v into a SQL literal for client-side binding, where
// placeholders like `?`, `$1`, and `@name` are replaced directly in the query
// text. Bools become `1`/`0` here, which is valid SQL. Server-side query
// parameters need `true`/`false` instead — see formatValue.
func format(tz *time.Location, scale TimeUnit, v any) (string, error) {
	return formatValue(tz, scale, v, false)
}

// formatValue turns v into a string. The boolAsText flag picks how bools are
// written:
//
//   - false: for client-side binding, where the value is spliced into the
//     query text. Bools become `1`/`0`.
//   - true: for server-side query parameters (`{name:Type}`). The server
//     parses these values as text, and its text parser only accepts
//     `true`/`false` for bools inside types like `Array(Bool)` — `1`/`0` is
//     rejected.
//
// The flag is passed down into nested values, so a bool inside an array, map,
// or tuple is formatted the same way at any depth.
func formatValue(tz *time.Location, scale TimeUnit, v any, boolAsText bool) (string, error) {
	quote := func(v string) string {
		return "'" + stringQuoteReplacer.Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return quote(v), nil
	case time.Time:
		return formatTime(tz, scale, v)
	case *time.Time:
		if v == nil {
			return "NULL", nil
		}
		return formatTime(tz, scale, *v)
	case bool:
		if boolAsText {
			if v {
				return "true", nil
			}
			return "false", nil
		}
		if v {
			return "1", nil
		}
		return "0", nil
	case float32:
		return formatFloat(float64(v), 32), nil
	case float64:
		return formatFloat(v, 64), nil
	case GroupSet:
		val, err := join(tz, scale, v.Value, boolAsText)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", val), nil
	case []GroupSet:
		val, err := join(tz, scale, v, boolAsText)
		if err != nil {
			return "", err
		}
		return val, err
	case ArraySet:
		val, err := join(tz, scale, v, boolAsText)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[%s]", val), nil
	case fmt.Stringer:
		if v := reflect.ValueOf(v); v.Kind() == reflect.Pointer &&
			v.IsNil() &&
			v.Type().Elem().Implements(reflect.TypeOf((*fmt.Stringer)(nil)).Elem()) {
			return "NULL", nil
		}
		return quote(v.String()), nil
	case column.OrderedMap:
		values := make([]string, 0)
		for key := range v.Keys() {
			name, err := formatValue(tz, scale, key, boolAsText)
			if err != nil {
				return "", err
			}
			value, _ := v.Get(key)
			val, err := formatValue(tz, scale, value, boolAsText)
			if err != nil {
				return "", err
			}
			values = append(values, fmt.Sprintf("%s, %s", name, val))
		}

		return "map(" + strings.Join(values, ", ") + ")", nil
	case column.IterableOrderedMap:
		values := make([]string, 0)
		iter := v.Iterator()
		for iter.Next() {
			key, value := iter.Key(), iter.Value()
			name, err := formatValue(tz, scale, key, boolAsText)
			if err != nil {
				return "", err
			}
			val, err := formatValue(tz, scale, value, boolAsText)
			if err != nil {
				return "", err
			}
			values = append(values, fmt.Sprintf("%s, %s", name, val))
		}

		return "map(" + strings.Join(values, ", ") + ")", nil
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		return quote(v.String()), nil
	case reflect.Slice, reflect.Array:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			val, err := formatValue(tz, scale, v.Index(i).Interface(), boolAsText)
			if err != nil {
				return "", err
			}
			values = append(values, val)
		}
		return fmt.Sprintf("[%s]", strings.Join(values, ", ")), nil
	case reflect.Map: // map
		values := make([]string, 0, len(v.MapKeys()))
		for _, key := range v.MapKeys() {
			name := fmt.Sprint(key.Interface())
			if key.Kind() == reflect.String {
				name = fmt.Sprintf("'%s'", name)
			}
			val, err := formatValue(tz, scale, v.MapIndex(key).Interface(), boolAsText)
			if err != nil {
				return "", err
			}
			values = append(values, fmt.Sprintf("%s, %s", name, val))
		}
		return "map(" + strings.Join(values, ", ") + ")", nil
	case reflect.Float32:
		return formatFloat(v.Float(), 32), nil
	case reflect.Float64:
		return formatFloat(v.Float(), 64), nil
	case reflect.Ptr:
		if v.IsNil() {
			return "NULL", nil
		}
		return formatValue(tz, scale, v.Elem().Interface(), boolAsText)
	}
	return fmt.Sprint(v), nil
}

// formatFloat renders a float as a CAST to the matching ClickHouse Float type.
// Without the cast, integer-valued floats like 1.0 render as the bare literal
// "1", which ClickHouse infers as an integer and later narrows (breaking typed
// float scans). NaN and infinities are quoted in the lowercase form ClickHouse
// accepts, since Go's default formatting ("NaN", "+Inf") is not valid SQL.
func formatFloat(f float64, bitSize int) string {
	chType := "Float64"
	if bitSize == 32 {
		chType = "Float32"
	}
	switch {
	case math.IsNaN(f):
		return fmt.Sprintf("cast('nan', '%s')", chType)
	case math.IsInf(f, 1):
		return fmt.Sprintf("cast('inf', '%s')", chType)
	case math.IsInf(f, -1):
		return fmt.Sprintf("cast('-inf', '%s')", chType)
	}
	return fmt.Sprintf("cast(%s, '%s')", strconv.FormatFloat(f, 'g', -1, bitSize), chType)
}

func join[E any](tz *time.Location, scale TimeUnit, values []E, boolAsText bool) (string, error) {
	items := make([]string, len(values))
	for i := range values {
		val, err := formatValue(tz, scale, values[i], boolAsText)
		if err != nil {
			return "", err
		}
		items[i] = val
	}
	return strings.Join(items, ", "), nil
}

func rebind(in []std_driver.NamedValue) []any {
	args := make([]any, 0, len(in))
	for _, v := range in {
		switch {
		case len(v.Name) != 0:
			args = append(args, driver.NamedValue{
				Name:  v.Name,
				Value: v.Value,
			})

		default:
			args = append(args, v.Value)
		}
	}
	return args
}
