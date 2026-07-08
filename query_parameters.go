package clickhouse

import (
	"errors"
	"regexp"
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
				// Strings and times at the top level are sent raw, without
				// quotes: the server reads a whole parameter value as-is,
				// and only quotes values nested inside arrays, maps, and
				// tuples. formatValue below applies the nested (quoted)
				// rules, so these skip it. Nil pointers fall through and
				// format as NULL.
				switch v := p.Value.(type) {
				case string:
					options.parameters[p.Name] = v
					continue
				case *string:
					if v != nil {
						options.parameters[p.Name] = *v
						continue
					}
				case time.Time:
					options.parameters[p.Name] = formatTimeParam(v)
					continue
				case *time.Time:
					if v != nil {
						options.parameters[p.Name] = formatTimeParam(*v)
						continue
					}
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

// formatTimeParam renders a time.Time sent as a query parameter through
// Named (which, unlike NamedDateValue, carries no scale). Whole-second times
// use the plain DateTime form. Sub-second times keep their fraction, trimmed
// to milli/micro/nanoseconds: a DateTime64 parameter preserves the precision
// (the server drops digits beyond its declared scale), while a plain
// DateTime parameter rejects the value with an error — better than silently
// truncating it. Use DateNamed to pin an exact scale.
func formatTimeParam(t time.Time) string {
	switch ns := t.Nanosecond(); {
	case ns == 0:
		return t.Format("2006-01-02 15:04:05")
	case ns%1e6 == 0:
		return t.Format("2006-01-02 15:04:05.000")
	case ns%1e3 == 0:
		return t.Format("2006-01-02 15:04:05.000000")
	default:
		return t.Format("2006-01-02 15:04:05.000000000")
	}
}

func formatTimeWithScale(t time.Time, scale TimeUnit) string {
	switch scale {
	case MicroSeconds:
		return t.Format("2006-01-02 15:04:05.000000")
	case MilliSeconds:
		return t.Format("2006-01-02 15:04:05.000")
	case NanoSeconds:
		return t.Format("2006-01-02 15:04:05.000000000")
	default:
		return t.Format("2006-01-02 15:04:05")
	}
}
