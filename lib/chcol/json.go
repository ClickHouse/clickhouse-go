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

package chcol

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// JSON represents a ClickHouse JSON type that can hold multiple possible types
type JSON struct {
	valuesByPath map[string]any
}

// NewJSON creates a new empty JSON value
func NewJSON() *JSON {
	return &JSON{
		valuesByPath: make(map[string]any),
	}
}

func (o *JSON) ValuesByPath() map[string]any {
	return o.valuesByPath
}

func (o *JSON) SetValueAtPath(path string, value any) {
	o.valuesByPath[path] = value
}

func (o *JSON) ValueAtPath(path string) (any, bool) {
	value, ok := o.valuesByPath[path]
	return value, ok
}

// MarshalJSON implements the json.Marshaler interface
func (o *JSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.valuesByPath)
}

// Scan implements the sql.Scanner interface
func (o *JSON) Scan(value interface{}) error {
	valuesByPath, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("JSON Scan value must be map[string]any")
	}

	o.valuesByPath = valuesByPath
	return nil
}

// Value implements the driver.Valuer interface
func (o *JSON) Value() (driver.Value, error) {
	return o.valuesByPath, nil
}
