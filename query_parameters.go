package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	ErrInvalidValueInNamedDateValue = errors.New("invalid value in NamedDateValue for query parameter")
	ErrUnsupportedQueryParameter    = errors.New("unsupported query parameter type")

	hasQueryParamsRe = regexp.MustCompile("{.+:.+}")
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
				// Strings and times at the top level are sent raw, without
				// quotes: the server reads a whole parameter value as-is,
				// and only quotes values nested inside arrays, maps, and
				// tuples. formatValue below applies the nested (quoted)
				// rules, so these skip it.
				switch v := p.Value.(type) {
				case string:
					options.parameters[p.Name] = v
					continue
				case *string:
					options.parameters[p.Name] = *v
					continue
				case time.Time:
					options.parameters[p.Name] = formatTimeParam(v)
					continue
				case *time.Time:
					options.parameters[p.Name] = formatTimeParam(*v)
					continue
				}
				strVal, err := formatValue(timezone, Seconds, p.Value, formatParamText)
				if err != nil {
					return "", err
				}
				options.parameters[p.Name] = strVal

			case driver.NamedDateValue:
				if !p.Value.IsZero() && p.Name != "" {
					formatted := formatTimeWithScale(p.Value, TimeUnit(p.Scale))
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

// formatEpoch renders t as epoch seconds with exactly digits fractional
// digits (0, 3, 6 or 9), truncating anything finer. Query parameters are
// sent as epoch rather than wall-clock text: parameter text has no syntax
// for a timezone, so a wall-clock string would be re-interpreted in the
// parameter's declared zone and shift the instant whenever that zone differs
// from the value's — epoch is zone-free, so the instant always survives.
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
	// Unix() floors and Nanosecond() is the positive offset within that
	// second, so a pre-1970 instant like -86400.5 arrives as sec -86401,
	// ns 5e8. Printing those digits naively would yield "-86401.500" =
	// -86401.5 — carry the sign into both parts instead.
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

// formatTimeParam renders a time.Time sent as a query parameter through
// Named (which, unlike NamedDateValue, carries no scale), so the fraction
// width is inferred from the value: whole-second times emit just the
// integer, sub-second times keep their fraction. A DateTime64 parameter
// preserves the precision (the server drops digits beyond its declared
// scale), while a plain DateTime parameter rejects a fraction with an
// error — better than silently truncating it. Use DateNamed to pin an
// exact scale.
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

// formatTimeWithScale renders a time.Time sent as a query parameter through
// DateNamed: the fraction width is pinned by the explicit scale (Seconds →
// none, MilliSeconds → 3 digits, …), truncating any finer precision the
// value carries.
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
