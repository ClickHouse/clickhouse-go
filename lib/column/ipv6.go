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
	"net"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type IPv6 struct {
	data []byte
	name string
}

func (col *IPv6) Name() string {
	return col.name
}

func (col *IPv6) Type() Type {
	return "IPv6"
}

func (col *IPv6) ScanType() reflect.Type {
	return scanTypeIP
}

func (col *IPv6) Rows() int {
	return len(col.data) / net.IPv6len
}

func (col *IPv6) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *IPv6) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row).String()
	case **string:
		*d = new(string)
		**d = col.row(row).String()
	case *net.IP:
		*d = col.row(row)
	case **net.IP:
		*d = new(net.IP)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "IPv6",
		}
	}
	return nil
}

// appendIPv6Str appends bytes of the IPv6-formatted string to result byte array.
// If IP is not valid V4 error will be returned.
func appendIPv6Str(data []byte, strIp string) ([]byte, error) {
	ip := net.ParseIP(strIp)
	if ip == nil {
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv6",
			Hint: "invalid IP format",
		}
	}
	return appendIPv6(data, ip)
}

// appendIPv6 appends bytes of IPv6 to result byte array.
// If IP is not valid V4 error will be returned.
func appendIPv6(data []byte, ip net.IP) ([]byte, error) {
	ip = ip.To16()
	if ip == nil {
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv6",
			Hint: "invalid IP version",
		}
	}
	return append(data, ip[:]...), nil
}

func (col *IPv6) Append(v interface{}) (nulls []uint8, err error) {
	var data []byte

	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			data, err = appendIPv6Str(data, v)
			if err != nil {
				return
			}
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				data, err = appendIPv6Str(data, *v)
				if err != nil {
					return
				}
			default:
				data, nulls[i] = append(data, make([]byte, net.IPv6len)...), 1
			}
		}
	case []net.IP:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			data, err = appendIPv6(data, v)
			if err != nil {
				return
			}
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				data, err = appendIPv6(data, *v)
				if err != nil {
					return
				}
			default:
				data, nulls[i] = append(data, make([]byte, net.IPv6len)...), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv6",
			From: fmt.Sprintf("%T", v),
		}
	}

	col.data = append(col.data, data...)
	return
}

func (col *IPv6) AppendRow(v interface{}) (err error) {
	switch v := v.(type) {
	case string:
		col.data, err = appendIPv6Str(col.data, v)
	case *string:
		switch {
		case v != nil:
			col.data, err = appendIPv6Str(col.data, *v)
		default:
			col.data, err = appendIPv6(col.data, make(net.IP, net.IPv6len))
		}
	case net.IP:
		col.data, err = appendIPv6(col.data, v)
	case *net.IP:
		switch {
		case v != nil:
			col.data, err = appendIPv6(col.data, *v)
		default:
			col.data, err = appendIPv6(col.data, make(net.IP, net.IPv6len))
		}
	case nil:
		col.data, err = appendIPv6(col.data, make(net.IP, net.IPv6len))
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "IPv6",
			From: fmt.Sprintf("%T", v),
		}
	}

	return
}

func (col *IPv6) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, net.IPv6len*rows)
	return decoder.Raw(col.data)
}

func (col *IPv6) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *IPv6) row(i int) net.IP {
	return col.data[i*net.IPv6len : (i+1)*net.IPv6len]
}

var _ Interface = (*IPv6)(nil)
