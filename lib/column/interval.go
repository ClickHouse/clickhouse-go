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
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Interval struct {
	chType Type
	values Int64
	name   string
}

func (col *Interval) Name() string {
	return col.name
}

func (col *Interval) parse(t Type) (Interface, error) {
	switch col.chType = t; col.chType {
	case "IntervalSecond", "IntervalMinute", "IntervalHour", "IntervalDay", "IntervalWeek", "IntervalMonth", "IntervalYear":
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Interval) Type() Type             { return col.chType }
func (col *Interval) ScanType() reflect.Type { return scanTypeString }
func (col *Interval) Rows() int              { return len(col.values.data) }
func (col *Interval) Row(i int, ptr bool) interface{} {
	return col.row(i)
}
func (col *Interval) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row)
	case **string:
		*d = new(string)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Interval",
		}
	}
	return nil
}

func (Interval) Append(interface{}) ([]uint8, error) {
	return nil, &Error{
		ColumnType: "Interval",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}

func (Interval) AppendRow(interface{}) error {
	return &Error{
		ColumnType: "Interval",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}

func (col *Interval) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (Interval) Encode(*binary.Encoder) error {
	return &Error{
		ColumnType: "Interval",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}

func (col *Interval) row(i int) string {
	v := fmt.Sprintf("%d %s", col.values.data[i], strings.TrimPrefix(string(col.chType), "Interval"))
	if col.values.data[i] > 1 {
		v += "s"
	}
	return v
}

var _ Interface = (*Interval)(nil)
