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

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/paulmach/orb"
)

type Point struct {
	lon  Float64
	lat  Float64
	name string
}

func (col *Point) Name() string {
	return col.name
}

func (col *Point) Type() Type {
	return "Point"
}

func (col *Point) ScanType() reflect.Type {
	return scanTypePoint
}

func (col *Point) Rows() int {
	return col.lon.Rows()
}

func (col *Point) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Point) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *orb.Point:
		*d = col.row(row)
	case **orb.Point:
		*d = new(orb.Point)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Point",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *Point) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []orb.Point:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.lon.data = append(col.lon.data, v.Lon())
			col.lat.data = append(col.lat.data, v.Lat())
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Point",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}
func (col *Point) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case orb.Point:
		col.lon.data = append(col.lon.data, v.Lon())
		col.lat.data = append(col.lat.data, v.Lat())
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Point",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Point) Decode(decoder *binary.Decoder, rows int) error {
	if err := col.lon.Decode(decoder, rows); err != nil {
		return err
	}
	if err := col.lat.Decode(decoder, rows); err != nil {
		return err
	}
	return nil
}

func (col *Point) Encode(encoder *binary.Encoder) error {
	if err := col.lon.Encode(encoder); err != nil {
		return err
	}
	return col.lat.Encode(encoder)
}

func (col *Point) row(i int) orb.Point {
	return orb.Point{
		col.lon.data[i],
		col.lat.data[i],
	}
}

var _ Interface = (*Point)(nil)
