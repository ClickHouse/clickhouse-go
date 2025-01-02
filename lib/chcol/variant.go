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
)

// Variant represents a ClickHouse Variant type that can hold multiple possible types
type Variant struct {
	value any
}

// NewVariant creates a new Variant with the given value
func NewVariant(v any) Variant {
	return Variant{value: v}
}

// Nil returns true if the underlying value is nil.
func (v Variant) Nil() bool {
	return v.value == nil
}

// Any returns the underlying value as any. Same as Interface.
func (v Variant) Any() any {
	return v.value
}

// Interface returns the underlying value as interface{}. Same as Any.
func (v Variant) Interface() interface{} {
	return v.value
}

// Int returns the value as an int if possible
func (v Variant) Int() (int, bool) {
	if i, ok := v.value.(int); ok {
		return i, true
	}

	return 0, false
}

// Int64 returns the value as an int64 if possible
func (v Variant) Int64() (int64, bool) {
	if i, ok := v.value.(int64); ok {
		return i, true
	}

	return 0, false
}

// String returns the value as a string if possible
func (v Variant) String() (string, bool) {
	if s, ok := v.value.(string); ok {
		return s, true
	}

	return "", false
}

// Bool returns the value as an bool if possible
func (v Variant) Bool() (bool, bool) {
	if b, ok := v.value.(bool); ok {
		return b, true
	}

	return false, false
}

// MarshalJSON implements the json.Marshaler interface
func (v *Variant) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

// Scan implements the sql.Scanner interface
func (v *Variant) Scan(value interface{}) error {
	v.value = value
	return nil
}

// Value implements the driver.Valuer interface
func (v Variant) Value() (driver.Value, error) {
	return v.value, nil
}

func (v Variant) WithType(chType string) VariantWithType {
	return VariantWithType{
		Variant: v,
		chType:  chType,
	}
}

// VariantWithType is Variant with an extra value for specifying the preferred ClickHouse type for column encoding
type VariantWithType struct {
	Variant
	chType string
}

// NewVariantWithType creates a new Variant with the given value and ClickHouse type
func NewVariantWithType(v any, chType string) VariantWithType {
	return VariantWithType{
		Variant: Variant{value: v},
		chType:  chType,
	}
}

// Type returns the ClickHouse type as a string.
func (v VariantWithType) Type() string {
	return v.chType
}
