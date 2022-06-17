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
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Tuple struct {
	chType  Type
	columns []Interface
	name    string
}

func (col *Tuple) Name() string {
	return col.name
}

type namedCol struct {
	name    string
	colType Type
}

func (col *Tuple) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var (
		element       []rune
		elements      []namedCol
		brackets      int
		appendElement = func() {
			if len(element) != 0 {
				cType := strings.TrimSpace(string(element))
				name := ""
				if parts := strings.SplitN(cType, " ", 2); len(parts) == 2 {
					if !strings.Contains(parts[0], "(") {
						name = parts[0]
						cType = parts[1]
					}
				}
				elements = append(elements, namedCol{
					name:    name,
					colType: Type(strings.TrimSpace(cType)),
				})
			}
		}
	)
	for _, r := range t.params() {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ',':
			if brackets == 0 {
				appendElement()
				element = element[:0]
				continue
			}
		}
		element = append(element, r)
	}
	appendElement()
	for _, ct := range elements {
		column, err := ct.colType.Column(ct.name)
		if err != nil {
			return nil, err
		}
		col.columns = append(col.columns, column)
	}
	if len(col.columns) != 0 {
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Tuple) Type() Type {
	return col.chType
}

func (Tuple) ScanType() reflect.Type {
	return scanTypeSlice
}

func (col *Tuple) Rows() int {
	if len(col.columns) != 0 {
		return col.columns[0].Rows()
	}
	return 0
}

func (col *Tuple) Row(i int, ptr bool) interface{} {
	tuple := make([]interface{}, 0, len(col.columns))
	for _, c := range col.columns {
		tuple = append(tuple, c.Row(i, ptr))
	}
	return tuple
}

func setJSONFieldValue(field reflect.Value, value reflect.Value) error {
	switch field.Interface().(type) {
	case time.Time:
		if value.Kind() == reflect.String {
			sValue := value.Interface().(string)
			val, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", sValue)
			if err != nil {
				return &Error{
					ColumnType: fmt.Sprint(field.Type()),
					Err:        fmt.Errorf("%s cannot be parsed into a time.Time as it isn't in the default format [2006-01-02 15:04:05.999999999 -0700 MST]", sValue),
				}
			}
			field.Set(reflect.ValueOf(val))
			return nil
		}
	case decimal.Decimal:
		if value.Kind() == reflect.String {
			sValue := value.Interface().(string)
			var val decimal.Decimal
			if sValue == "" {
				field.Set(reflect.ValueOf(val))
				return nil
			}
			val, err := decimal.NewFromString(sValue)
			if err != nil {
				return &Error{
					ColumnType: fmt.Sprint(field.Type()),
					Err:        fmt.Errorf("value %s but cannot be parsed into a decimal.Decimal - %s", sValue, err),
				}
			}
			field.Set(reflect.ValueOf(val))
			return nil
		}
	case net.IP:
		if value.Kind() == reflect.String {
			sValue := value.Interface().(string)
			field.Set(reflect.ValueOf(net.ParseIP(sValue)))
			return nil
		}
	case uuid.UUID:
		if value.Kind() == reflect.String {
			sValue := value.Interface().(string)
			uuid, err := uuid.Parse(sValue)
			if err != nil {
				return &Error{
					ColumnType: fmt.Sprint(field.Type()),
					Err:        fmt.Errorf("value %s cannot be parsed into a uuid.UUID - %s", sValue, err),
				}
			}
			field.Set(reflect.ValueOf(uuid))
			return nil
		}
	}

	// check if our target is a string
	if field.Kind() == reflect.String {
		field.Set(reflect.ValueOf(fmt.Sprint(value.Interface())))
		return nil
	}
	if value.CanConvert(field.Type()) {
		field.Set(value.Convert(field.Type()))
		return nil
	}

	return &ColumnConverterError{
		Op:   "ScanRow",
		To:   fmt.Sprintf("%T", field.Interface()),
		From: value.Type().String(),
	}

}

func getStructFieldValue(field reflect.Value, name string) (reflect.Value, bool) {
	tField := field.Type()
	for i := 0; i < tField.NumField(); i++ {
		if jsonTag := tField.Field(i).Tag.Get("json"); jsonTag == name {
			return field.Field(i), true
		}
	}
	sField := field.FieldByName(name)
	return sField, sField.IsValid()
}

func (col *Tuple) scanJSONMap(json reflect.Value, row int) error {
	if json.Type().Key().Kind() != reflect.String {
		return &Error{
			ColumnType: fmt.Sprint(json.Type().Key().Kind()),
			Err:        fmt.Errorf("column %s - map keys must be a string", col.Name()),
		}
	}
	for _, c := range col.columns {
		switch dCol := c.(type) {
		case *Tuple:
			switch json.Type().Elem().Kind() {
			case reflect.Struct:
				rStruct := reflect.New(json.Type().Elem()).Elem()
				if err := dCol.scanJSONStruct(rStruct, row); err != nil {
					return err
				}
				json.SetMapIndex(reflect.ValueOf(c.Name()), rStruct)
			case reflect.Map:
				// get a typed map
				newMap := reflect.MakeMap(json.Type().Elem())
				if err := dCol.scanJSONMap(newMap, row); err != nil {
					return err
				}
				json.SetMapIndex(reflect.ValueOf(c.Name()), newMap)
			case reflect.Interface:
				// catches interface{} - Note this swallows custom interfaces to which maps couldn't conform
				newMap := reflect.ValueOf(make(map[string]interface{}))
				if err := dCol.scanJSONMap(newMap, row); err != nil {
					return err
				}
				json.SetMapIndex(reflect.ValueOf(c.Name()), newMap)
			default:
				return &Error{
					ColumnType: fmt.Sprint(json.Type().Elem().Kind()),
					Err:        fmt.Errorf("column %s - needs a map/struct or interface{}", col.Name()),
				}
			}
		case *Nested:
			aCol := dCol.Interface.(*Array)
			subSlice, err := aCol.parseJSONSliceOfObjects(json.Type().Elem(), row)
			if err != nil {
				return err
			}
			// this wont work if json is a map[string][]interface{} and we try to set a typed slice
			json.SetMapIndex(reflect.ValueOf(c.Name()), subSlice)
		case *Array:
			switch dCol.values.(type) {
			case *Tuple:
				// eqv. of nested
				subSlice, err := dCol.parseJSONSliceOfObjects(json.Type().Elem(), row)
				if err != nil {
					return err
				}
				json.SetMapIndex(reflect.ValueOf(c.Name()), subSlice)
			default:
				// this will include nested Arrays which if primitive types can be nested deep
				switch json.Type().Elem().Kind() {
				case reflect.Slice:
					subSlice, err := dCol.scanJSONSlice(json.Type().Elem(), row, 0)
					if err != nil {
						return err
					}
					json.SetMapIndex(reflect.ValueOf(c.Name()), subSlice)
				case reflect.Interface:
					// we assume interface{} - any other custom interfaces will fail
					field := reflect.New(reflect.TypeOf(c.Row(0, false))).Elem()
					value := reflect.ValueOf(c.Row(row, false))
					if err := setJSONFieldValue(field, value); err != nil {
						return err
					}
					json.SetMapIndex(reflect.ValueOf(c.Name()), field)
				default:
					return &Error{
						ColumnType: fmt.Sprint(json.Type().Elem().Kind()),
						Err:        fmt.Errorf("column %s - needs a slice or interface{}", col.Name()),
					}
				}
			}
		default:
			field := reflect.New(reflect.TypeOf(c.Row(0, false))).Elem()
			value := reflect.ValueOf(c.Row(row, false))
			if err := setJSONFieldValue(field, value); err != nil {
				return err
			}
			json.SetMapIndex(reflect.ValueOf(c.Name()), field)
		}
	}
	return nil
}

func (col *Tuple) scanJSONStruct(json reflect.Value, row int) error {
	for _, c := range col.columns {
		// the column may be serialized using a different name due to a struct "json" tag
		sField, ok := getStructFieldValue(json, c.Name())
		// test if map
		if !ok {
			continue
		}
		switch dCol := c.(type) {
		case *Tuple:
			switch sField.Kind() {
			case reflect.Struct:
				if err := dCol.scanJSONStruct(sField, row); err != nil {
					return err
				}
			case reflect.Map:
				newMap := reflect.MakeMap(sField.Type())
				if err := dCol.scanJSONMap(newMap, row); err != nil {
					return err
				}
				sField.Set(newMap)
			case reflect.Interface:
				// catches []interface{} -Note this swallows custom interfaces to which maps couldn't conform
				newMap := reflect.ValueOf(make(map[string]interface{}))
				if err := dCol.scanJSONMap(newMap, row); err != nil {
					return err
				}
				sField.Set(newMap)
			default:
				return &Error{
					ColumnType: fmt.Sprint(sField.Kind()),
					Err:        fmt.Errorf("column %s - needs a map/struct or interface{}", col.Name()),
				}
			}
		case *Nested:
			aCol := dCol.Interface.(*Array)
			subSlice, err := aCol.parseJSONSliceOfObjects(sField.Type(), row)
			if err != nil {
				return err
			}
			sField.Set(subSlice)
		case *Array:
			switch dCol.values.(type) {
			case *Tuple:
				//eqv of nested
				subSlice, err := dCol.parseJSONSliceOfObjects(sField.Type(), row)
				if err != nil {
					return err
				}
				sField.Set(subSlice)
			default:
				// slice of primitives
				switch sField.Kind() {
				case reflect.Slice:
					subSlice, err := dCol.scanJSONSlice(sField.Type(), row, 0)
					if err != nil {
						return err
					}
					sField.Set(subSlice)
				case reflect.Interface:
					value := reflect.ValueOf(c.Row(row, false))
					if err := setJSONFieldValue(sField, value); err != nil {
						return err
					}
				default:
					return &Error{
						ColumnType: fmt.Sprint(json.Type().Elem().Kind()),
						Err:        fmt.Errorf("column %s - needs a slice or interface{}", col.Name()),
					}
				}

			}
		default:
			value := reflect.ValueOf(c.Row(row, false))
			if err := setJSONFieldValue(sField, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (col *Tuple) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *[]interface{}:
		tuple := make([]interface{}, 0, len(col.columns))
		for _, c := range col.columns {
			tuple = append(tuple, c.Row(row, false))
		}
		*d = tuple
	default:
		jType := reflect.Indirect(reflect.ValueOf(dest))
		kind := jType.Kind()
		if kind == reflect.Struct {
			rStruct := reflect.New(jType.Type()).Elem()
			err := col.scanJSONStruct(rStruct, row)
			if err != nil {
				return err
			}
			jType.Set(rStruct)
			return nil
		}
		if kind == reflect.Map {
			//check if pointer
			mapVal := reflect.Indirect(reflect.ValueOf(dest))
			if mapVal.IsNil() {
				//if not initialized
				newMap := reflect.MakeMap(mapVal.Type())
				if err := col.scanJSONMap(newMap, row); err != nil {
					return err
				}
				mapVal.Set(newMap)
				return nil
			}
			return col.scanJSONMap(mapVal, row)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(col.chType),
		}
	}
	return nil
}

func (col *Tuple) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case [][]interface{}:
		for _, v := range v {
			if err := col.AppendRow(v); err != nil {
				return nil, err
			}
		}
		return nil, nil
	case []*[]interface{}:
		for _, v := range v {
			if err := col.AppendRow(v); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	return nil, &ColumnConverterError{
		Op:   "Append",
		To:   string(col.chType),
		From: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case []interface{}:
		if len(v) != len(col.columns) {
			return &Error{
				ColumnType: string(col.chType),
				Err:        fmt.Errorf("invalid size. expected %d got %d", len(col.columns), len(v)),
			}
		}
		for i, v := range v {
			if err := col.columns[i].AppendRow(v); err != nil {
				return err
			}
		}
		return nil
	case *[]interface{}:
		if v == nil {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   string(col.chType),
				From: fmt.Sprintf("%T", v),
				Hint: "invalid (nil) pointer value",
			}
		}
		if len(*v) != len(col.columns) {
			return &Error{
				ColumnType: string(col.chType),
				Err:        fmt.Errorf("invalid size. expected %d got %d", len(col.columns), len(*v)),
			}
		}
		for i, v := range *v {
			if err := col.columns[i].AppendRow(v); err != nil {
				return err
			}
		}
		return nil
	}
	return &ColumnConverterError{
		Op:   "AppendRow",
		To:   string(col.chType),
		From: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) Decode(decoder *binary.Decoder, rows int) error {
	for _, c := range col.columns {
		if err := c.Decode(decoder, rows); err != nil {
			return err
		}
	}
	return nil
}

func (col *Tuple) Encode(encoder *binary.Encoder) error {
	for _, c := range col.columns {
		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Tuple) ReadStatePrefix(decoder *binary.Decoder) error {
	for _, c := range col.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.ReadStatePrefix(decoder); err != nil {
				return err
			}
		}
	}
	return nil
}

func (col *Tuple) WriteStatePrefix(encoder *binary.Encoder) error {
	for _, c := range col.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	_ Interface           = (*Tuple)(nil)
	_ CustomSerialization = (*Tuple)(nil)
)
