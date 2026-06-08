package clickhouse

import (
	"errors"
	"regexp"
	"strings"
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
				if str, ok := p.Value.(string); ok {
					options.parameters[p.Name] = str
					continue
				}
				// using the same format logic for NamedValue typed value in function bindNamed
				strVal, err := format(timezone, Seconds, p.Value)
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

// escapeQueryParam applies TSV escaping to a query parameter value.
// ClickHouse deserializes named query parameters using deserializeTextEscaped,
// so control characters must be escaped before sending over any transport.
// A raw tab (0x09) is treated as a TSV field delimiter and causes error 457.
func escapeQueryParam(v string) string {
	var sb strings.Builder
	sb.Grow(len(v))
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\\':
			sb.WriteString(`\\`)
		case '\t':
			sb.WriteString(`\t`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\000':
			sb.WriteString(`\0`)
		default:
			sb.WriteByte(v[i])
		}
	}
	return sb.String()
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
