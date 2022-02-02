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

type IPv4 struct {
	data []byte
}

func (col *IPv4) Type() Type {
	return "IPv4"
}

func (col *IPv4) ScanType() reflect.Type {
	return scanTypeIP
}

func (col *IPv4) Rows() int {
	return len(col.data) / net.IPv4len
}

func (col *IPv4) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *IPv4) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *net.IP:
		*d = col.row(row)
	case **net.IP:
		*d = new(net.IP)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "IPv4",
		}
	}
	return nil
}

func (col *IPv4) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []net.IP:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			ip := v.To4()
			if ip == nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "IPv4",
					From: "IPv6",
					Hint: "invalid IP version",
				}
			}
			col.data = append(col.data, IPv4ToBytes(ip)...)
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				ip := v.To4()
				if ip == nil {
					return nil, &ColumnConverterError{
						Op:   "Append",
						To:   "IPv4",
						From: "IPv6",
						Hint: "invalid IP version",
					}
				}
				col.data = append(col.data, IPv4ToBytes(ip)...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, net.IPv4len)...), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv4",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *IPv4) AppendRow(v interface{}) error {
	var ip net.IP
	switch v := v.(type) {
	case net.IP:
		ip = v
	case *net.IP:
		switch {
		case v != nil:
			ip = *v
		default:
			ip = make(net.IP, net.IPv4len)
		}
	case nil:
		ip = make(net.IP, net.IPv4len)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "IPv4",
			From: fmt.Sprintf("%T", v),
		}
	}
	data := ip.To4()
	if data == nil {
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "IPv4",
			From: "IPv6",
			Hint: "invalid IP version",
		}
	}
	col.data = append(col.data, IPv4ToBytes(data)...)
	return nil
}

func (col *IPv4) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, net.IPv4len*rows)
	return decoder.Raw(col.data)
}

func (col *IPv4) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *IPv4) row(i int) net.IP {
	src := col.data[i*net.IPv4len : (i+1)*net.IPv4len]
	return net.IPv4(src[3], src[2], src[1], src[0]).To4()
}

func IPv4ToBytes(ip net.IP) []byte {
	return []byte{ip[3], ip[2], ip[1], ip[0]}
}

var _ Interface = (*IPv4)(nil)
