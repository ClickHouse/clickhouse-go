package clickhouse

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
)

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

func bindNumeric(query string, args ...interface{}) (string, error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
	)
	for i, v := range args {
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

func bindNamed(query string, args ...interface{}) (string, error) {
	var (
		unbind = make(map[string]struct{})
		params = make(map[string]string)
	)
	for _, v := range args {
		switch v := v.(type) {
		case driver.NamedValue:
			params["@"+v.Name] = format(v.Value)
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
		return "", fmt.Errorf("have no arg for %s param", param)
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
