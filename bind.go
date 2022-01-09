package clickhouse

import (
	std_driver "database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
)

func Named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

func bind(query string, args ...interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var (
		haveNamed   bool
		haveNumeric bool
	)
	for _, v := range args {
		switch v.(type) {
		case driver.NamedValue:
			haveNamed = true
		default:
			haveNumeric = true
		}
		if haveNamed && haveNumeric {
			return "", fmt.Errorf("clickhouse bind: mixed named and numeric parameters")
		}
	}
	if haveNamed {
		return bindNamed(query, args...)
	}
	return bindNumeric(query, args...)
}

var bindNumericRe = regexp.MustCompile(`\$[0-9]+`)

func bindNumeric(query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
	)
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", nil
			}
		}
		params[fmt.Sprintf("$%d", i+1)] = format(v)
	}
	query = bindNumericRe.ReplaceAllStringFunc(query, func(n string) string {
		if _, found := params[n]; !found {
			unbind[n] = struct{}{}
			return ""
		}
		return params[n]
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %s param", param)
	}
	return query, nil
}

var bindNamedRe = regexp.MustCompile(`@[a-zA-Z0-9\_]+`)

func bindNamed(query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
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
			params["@"+v.Name] = format(value)
		}
	}
	query = bindNamedRe.ReplaceAllStringFunc(query, func(n string) string {
		if _, found := params[n]; !found {
			unbind[n] = struct{}{}
			return ""
		}
		return params[n]
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for %q param", param)
	}
	return query, nil
}

func format(v interface{}) string {
	quote := func(v string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return quote(v)
	case fmt.Stringer:
		return quote(v.String())
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Slice:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			values = append(values, format(v.Index(i).Interface()))
		}
		return strings.Join(values, ", ")
	}
	return fmt.Sprint(v)
}

func rebind(in []std_driver.NamedValue) []interface{} {
	args := make([]interface{}, 0, len(in))
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
