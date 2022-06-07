// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	std_driver "database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func Named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

var bindNumericRe = regexp.MustCompile(`\$[0-9]+`)
var bindPositionalRe = regexp.MustCompile(`[^\\][?]`)

func bind(tz *time.Location, query string, args ...interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var (
		haveNamed      bool
		haveNumeric    bool
		havePositional bool
	)
	haveNumeric = bindNumericRe.MatchString(query)
	havePositional = bindPositionalRe.MatchString(query)
	if haveNumeric && havePositional {
		return "", ErrBindMixedParamsFormats
	}
	for _, v := range args {
		switch v.(type) {
		case driver.NamedValue:
			haveNamed = true
		default:
		}
		if haveNamed && (haveNumeric || havePositional) {
			return "", ErrBindMixedParamsFormats
		}
	}
	if haveNamed {
		return bindNamed(tz, query, args...)
	}
	if haveNumeric {
		return bindNumeric(tz, query, args...)
	}
	return bindPositional(tz, query, args...)
}

var bindPositionCharRe = regexp.MustCompile(`[?]`)

func bindPositional(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
	var (
		unbind = make(map[int]struct{})
		params = make([]string, len(args))
	)
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", nil
			}
		}
		params[i], err = format(tz, v)
		if err != nil {
			return "", err
		}
	}
	i := 0
	query = bindPositionalRe.ReplaceAllStringFunc(query, func(n string) string {
		if i >= len(params) {
			unbind[i] = struct{}{}
			return ""
		}
		val := params[i]
		i++
		return bindPositionCharRe.ReplaceAllStringFunc(n, func(m string) string {
			return val
		})
	})
	for param := range unbind {
		return "", fmt.Errorf("have no arg for param ? at position %d", param)
	}
	// replace \? escape sequence
	return strings.ReplaceAll(query, "\\?", "?"), nil
}

func bindNumeric(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
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
		val, err := format(tz, v)
		if err != nil {
			return "", err
		}
		params[fmt.Sprintf("$%d", i+1)] = val
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

func bindNamed(tz *time.Location, query string, args ...interface{}) (_ string, err error) {
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
			val, err := format(tz, value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
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

func format(tz *time.Location, v interface{}) (string, error) {
	quote := func(v string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return quote(v), nil
	case time.Time:
		switch v.Location().String() {
		case "Local":
			return fmt.Sprintf("toDateTime(%d)", v.Unix()), nil
		case tz.String():
			return v.Format("toDateTime('2006-01-02 15:04:05')"), nil
		}
		return v.Format("toDateTime('2006-01-02 15:04:05', '" + v.Location().String() + "')"), nil
	case []interface{}: // tuple
		elements := make([]string, 0, len(v))
		for _, e := range v {
			val, err := format(tz, e)
			if err != nil {
				return "", err
			}
			elements = append(elements, val)
		}
		return "(" + strings.Join(elements, ", ") + ")", nil
	case [][]interface{}:
		items := make([]string, 0, len(v))
		for _, t := range v {
			val, err := format(tz, t)
			if err != nil {
				return "", err
			}
			items = append(items, val)
		}
		return strings.Join(items, ", "), nil
	case fmt.Stringer:
		return quote(v.String()), nil
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		return quote(v.String()), nil
	case reflect.Slice:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			val, err := format(tz, v.Index(i).Interface())
			if err != nil {
				return "", err
			}
			values = append(values, val)
		}
		return strings.Join(values, ", "), nil
	case reflect.Map: // map
		values := make([]string, 0, len(v.MapKeys()))
		for _, key := range v.MapKeys() {
			name := fmt.Sprint(key.Interface())
			if key.Kind() == reflect.String {
				name = fmt.Sprintf("'%s'", name)
			}
			val, err := format(tz, v.MapIndex(key).Interface())
			if err != nil {
				return "", err
			}
			if v.MapIndex(key).Kind() == reflect.Slice {
				// assume slices in maps are arrays
				val = fmt.Sprintf("[%s]", val)
			}
			values = append(values, fmt.Sprintf("%s : %s", name, val))
		}
		return "{" + strings.Join(values, ", ") + "}", nil

	}
	return fmt.Sprint(v), nil
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
