package clickhouse

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	ErrInvalidValueInNamedDateValue = errors.New("invalid value in NamedDateValue for query parameter")
	ErrUnsupportedQueryParameter    = errors.New("unsupported query parameter type")

	hasQueryParamsRe      = regexp.MustCompile("{.+:.+}")
	bindStartPositionalRe = regexp.MustCompile(`^[?]`) // matches ? at the start of the query
)

// hasParameterWrappers checks if any args are Parameter types
func hasParameterWrappers(args ...any) bool {
	for _, arg := range args {
		if _, ok := arg.(Parameter); ok {
			return true
		}
	}
	return false
}

// convertToServerSideBinding converts a query with ? or $N placeholders to {param_N:Type} format
// and builds the parameters map from Parameter-wrapped values
func convertToServerSideBinding(query string, timezone *time.Location, args ...any) (string, Parameters, error) {
	params := make(Parameters, len(args))

	// Detect which placeholder format is used
	hasNumeric := bindNumericRe.MatchString(query)
	hasPositional := bindPositionalRe.MatchString(query) || bindStartPositionalRe.MatchString(query)

	if hasNumeric && hasPositional {
		return "", nil, ErrBindMixedParamsFormats
	}

	if hasNumeric {
		// Convert $N placeholders to {param_N:Type}
		return convertNumericToServerSide(query, timezone, params, args...)
	}

	// Convert ? placeholders to {param_N:Type}
	return convertPositionalToServerSide(query, timezone, params, args...)
}

// convertNumericToServerSide converts $N placeholders to {param_N:Type}
func convertNumericToServerSide(query string, timezone *time.Location, params Parameters, args ...any) (string, Parameters, error) {
	// Build a map of parameter index to Parameter value
	paramMap := make(map[int]Parameter)
	for i, arg := range args {
		if p, ok := arg.(Parameter); ok {
			paramMap[i+1] = p // $N is 1-indexed
		} else {
			return "", nil, errors.New("all arguments must be Parameter types when using server-side binding")
		}
	}

	// Replace $N with {param_N:Type}
	converted := bindNumericRe.ReplaceAllStringFunc(query, func(match string) string {
		// Extract the number from $N
		numStr := match[1:] // skip the $
		var paramIdx int
		fmt.Sscanf(numStr, "%d", &paramIdx)

		if p, ok := paramMap[paramIdx]; ok {
			paramName := fmt.Sprintf("param_%d", paramIdx)
			// Format the value and store in params map
			formatted, err := format(timezone, Seconds, p.Value)
			if err != nil {
				return match // fallback to original on error
			}
			params[paramName] = formatted
			return fmt.Sprintf("{%s:%s}", paramName, p.CHType)
		}
		return match
	})

	return converted, params, nil
}

// convertPositionalToServerSide converts ? placeholders to {param_N:Type}
func convertPositionalToServerSide(query string, timezone *time.Location, params Parameters, args ...any) (string, Parameters, error) {
	// Validate all args are Parameter types
	for _, arg := range args {
		if _, ok := arg.(Parameter); !ok {
			return "", nil, errors.New("all arguments must be Parameter types when using server-side binding")
		}
	}

	paramIdx := 0
	var result strings.Builder
	i := 0

	for i < len(query) {
		// Check for escaped ? (\?)
		if i < len(query)-1 && query[i] == '\\' && query[i+1] == '?' {
			result.WriteByte('\\')
			result.WriteByte('?')
			i += 2
			continue
		}

		// Check for ? placeholder
		if query[i] == '?' {
			if paramIdx >= len(args) {
				return "", nil, errors.New("not enough arguments for placeholders")
			}

			p := args[paramIdx].(Parameter)
			paramName := fmt.Sprintf("param_%d", paramIdx+1)

			// Format the value and store in params map
			formatted, err := format(timezone, Seconds, p.Value)
			if err != nil {
				return "", nil, err
			}
			params[paramName] = formatted

			// Replace ? with {param_N:Type}
			result.WriteString(fmt.Sprintf("{%s:%s}", paramName, p.CHType))
			paramIdx++
			i++
			continue
		}

		result.WriteByte(query[i])
		i++
	}

	if paramIdx != len(args) {
		return "", nil, errors.New("number of placeholders does not match number of arguments")
	}

	return result.String(), params, nil
}

func bindQueryOrAppendParameters(paramsProtocolSupport bool, options *QueryOptions, query string, timezone *time.Location, args ...any) (string, error) {
	// prefer native query parameters over legacy bind if query parameters provided explicit
	if len(options.parameters) > 0 {
		return query, nil
	}

	// Check if we should convert ? or $N placeholders to server-side binding
	// This happens when:
	// 1. Server supports parameters (protocol version check)
	// 2. At least one argument is a Parameter wrapper
	// 3. Query doesn't already use {name:Type} syntax
	if paramsProtocolSupport &&
		len(args) > 0 &&
		hasParameterWrappers(args...) &&
		!hasQueryParamsRe.MatchString(query) {
		convertedQuery, params, err := convertToServerSideBinding(query, timezone, args...)
		if err != nil {
			return "", err
		}
		options.parameters = params
		return convertedQuery, nil
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
