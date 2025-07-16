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

package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

// Decoding (Scanning)

// scanIntoStruct will iterate the provided struct and scan JSON data into the matching fields
func (c *JSON_v1) scanIntoStruct(dest any, row int) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Pointer {
		return fmt.Errorf("destination must be a pointer")
	}
	val = val.Elem()

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a pointer to struct")
	}

	return c.fillStruct(val, "", row)
}

// scanIntoMap converts JSON data into a map
func (c *JSON_v1) scanIntoMap(dest any, row int) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Pointer {
		return fmt.Errorf("destination must be a pointer")
	}
	val = val.Elem()

	if val.Kind() != reflect.Map {
		return fmt.Errorf("destination must be a pointer to map")
	}

	if val.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("map key must be string")
	}

	if val.IsNil() {
		val.Set(reflect.MakeMap(val.Type()))
	}

	return c.fillMap(val, "", row)
}

// fillStruct will iterate the provided struct and scan JSON data into the matching fields recursively
func (c *JSON_v1) fillStruct(val reflect.Value, prefix string, row int) error {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanSet() {
			continue
		}

		name := fieldType.Tag.Get("json")
		if name == "" || name[0] == ',' {
			name = fieldType.Name
		} else {
			name = strings.Split(name, ",")[0]
		}

		if name == "-" {
			continue
		}

		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		if c.hasTypedPath(path) {
			err := c.scanTypedPathToValue(path, row, field)
			if err != nil {
				return fmt.Errorf("fillStruct failed to scan typed path: %w", err)
			}

			continue
		} else if c.hasDynamicPath(path) {
			err := c.scanDynamicPathToValue(path, row, field)
			if err != nil {
				return fmt.Errorf("fillStruct failed to scan dynamic path: %w", err)
			}

			continue
		}

		hasNestedFields := c.pathHasNestedValues(path)
		if !hasNestedFields {
			continue
		}

		switch field.Kind() {
		case reflect.Pointer:
			if field.Type().Elem().Kind() == reflect.Struct {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}

				if err := c.fillStruct(field.Elem(), path, row); err != nil {
					return fmt.Errorf("error filling nested struct pointer: %w", err)
				}
			}
		case reflect.Struct:
			if err := c.fillStruct(field, path, row); err != nil {
				return fmt.Errorf("error filling nested struct: %w", err)
			}
		case reflect.Map:
			if err := c.fillMap(field, path, row); err != nil {
				return fmt.Errorf("error filling nested map: %w", err)
			}
		}
	}

	return nil
}

// fillMap will iterate the provided map and scan JSON data in recursively
func (c *JSON_v1) fillMap(val reflect.Value, prefix string, row int) error {
	if val.IsNil() {
		val.Set(reflect.MakeMap(val.Type()))
	}

	var paths []string
	for _, path := range c.typedPaths {
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}
	for _, path := range c.dynamicPaths {
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}

	children := make(map[string][]string)
	prefixLen := len(prefix)
	if prefixLen > 0 {
		prefixLen++ // splitter
	}

	for _, path := range paths {
		if prefixLen >= len(path) {
			continue
		}

		suffix := path[prefixLen:]
		nextDot := strings.Index(suffix, ".")
		var current string
		if nextDot == -1 {
			current = suffix
		} else {
			current = suffix[:nextDot]
		}
		children[current] = append(children[current], path)
	}

	for key, childPaths := range children {
		noChildNodes := true
		for _, path := range childPaths {
			if strings.Contains(path[prefixLen:], ".") {
				noChildNodes = false
				break
			}
		}

		if noChildNodes {
			fullPath := prefix
			if prefix != "" {
				fullPath += "."
			}
			fullPath += key

			mapValueType := val.Type().Elem()
			newVal := reflect.New(mapValueType).Elem()

			var err error
			if _, isTyped := c.typedPathsIndex[fullPath]; isTyped {
				err = c.scanTypedPathToValue(fullPath, row, newVal)
			} else {
				if mapValueType.Kind() == reflect.Interface {
					value := c.valueAtPath(fullPath, row, false)
					if dyn, ok := value.(chcol.Dynamic); ok {
						value = dyn.Any()
					}

					if value != nil {
						newVal.Set(reflect.ValueOf(value))
					}
				} else {
					err = c.scanDynamicPathToValue(fullPath, row, newVal)
				}
			}
			if err != nil {
				return fmt.Errorf("failed to scan value at path \"%s\": %w", fullPath, err)
			}

			val.SetMapIndex(reflect.ValueOf(key), newVal)
		} else {
			newPrefix := prefix
			if newPrefix != "" {
				newPrefix += "."
			}
			newPrefix += key

			mapValueType := val.Type().Elem()
			var newMap reflect.Value

			if mapValueType.Kind() == reflect.Interface {
				newMap = reflect.MakeMap(reflect.TypeOf(map[string]interface{}{}))
			} else if mapValueType.Kind() == reflect.Map {
				newMap = reflect.MakeMap(mapValueType)
			} else {
				return fmt.Errorf("invalid map value type for nested path \"%s\"", newPrefix)
			}

			err := c.fillMap(newMap, newPrefix, row)
			if err != nil {
				return fmt.Errorf("failed filling nested map at path \"%s\": %w", newPrefix, err)
			}

			val.SetMapIndex(reflect.ValueOf(key), newMap)
		}
	}

	return nil
}
