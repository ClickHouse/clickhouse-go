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

func bind(tz *time.Location, query string, args ...interface{}) (string, error) {
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
			return "", ErrBindMixedNamedAndNumericParams
		}
	}
	if haveNamed {
		return bindNamed(tz, query, args...)
	}
	return bindNumeric(tz, query, args...)
}

var bindNumericRe = regexp.MustCompile(`\$[0-9]+`)

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
		params[fmt.Sprintf("$%d", i+1)] = format(tz, v)
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
			params["@"+v.Name] = format(tz, value)
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

func format(tz *time.Location, v interface{}) string {
	quote := func(v string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return quote(v)
	case time.Time:
		switch v.Location().String() {
		case "Local":
			return fmt.Sprintf("toDateTime(%d)", v.Unix())
		case tz.String():
			return v.Format("toDateTime('2006-01-02 15:04:05')")
		}
		return v.Format("toDateTime('2006-01-02 15:04:05', '" + v.Location().String() + "')")
	case []interface{}: // tuple
		elements := make([]string, 0, len(v))
		for _, e := range v {
			elements = append(elements, format(tz, e))
		}
		return "(" + strings.Join(elements, ", ") + ")"
	case [][]interface{}:
		items := make([]string, 0, len(v))
		for _, t := range v {
			items = append(items, format(tz, t))
		}
		return strings.Join(items, ", ")
	case fmt.Stringer:
		return quote(v.String())
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		return quote(v.String())
	case reflect.Slice:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			values = append(values, format(tz, v.Index(i).Interface()))
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
