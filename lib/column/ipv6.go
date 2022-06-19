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

func (col *IPv6) appendIPv6(ip net.IP) (err error) {
	col.data, err = appendIPv6(col.data, ip)
	return
}

func (col *IPv6) appendIPv6Str(strIp string) (err error) {
	col.data, err = appendIPv6Str(col.data, strIp)
	return
}

func (col *IPv6) appendEmptyIPv6() error {
	return col.appendIPv6(make(net.IP, net.IPv6len))
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
		err = col.appendIPv6Str(v)
	case *string:
		switch {
		case v != nil:
			err = col.appendIPv6Str(*v)
		default:
			err = col.appendEmptyIPv6()
		}
	case net.IP:
		err = col.appendIPv6(v)
	case *net.IP:
		switch {
		case v != nil:
			err = col.appendIPv6(*v)
		default:
			err = col.appendEmptyIPv6()
		}

	case nil:
		err = col.appendEmptyIPv6()
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
