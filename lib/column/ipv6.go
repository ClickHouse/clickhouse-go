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

func (col *IPv6) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []net.IP:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			if len(v) != net.IPv6len {
				return nil, &Error{
					ColumnType: string(col.Type()),
					Err:        fmt.Errorf("invalid size. expected %d got %d", net.IPv6len, len(v)),
				}
			}
			col.data = append(col.data, v[:]...)
		}
	case []*net.IP:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if len(*v) != net.IPv6len {
					return nil, &Error{
						ColumnType: string(col.Type()),
						Err:        fmt.Errorf("invalid size. expected %d got %d", net.IPv6len, len(*v)),
					}
				}
				tmp := *v
				col.data = append(col.data, tmp[:]...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, net.IPv6len)...), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "IPv6",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *IPv6) AppendRow(v interface{}) error {
	var ip net.IP
	switch v := v.(type) {
	case net.IP:
		ip = v
	case *net.IP:
		switch {
		case v != nil:
			ip = *v
		default:
			ip = make(net.IP, net.IPv6len)
		}
	case nil:
		ip = make(net.IP, net.IPv6len)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "IPv6",
			From: fmt.Sprintf("%T", v),
		}
	}
	if len(ip) != net.IPv6len {
		return &Error{
			ColumnType: string(col.Type()),
			Err:        fmt.Errorf("invalid size. expected %d got %d", net.IPv6len, len(ip)),
		}
	}
	col.data = append(col.data, ip[:]...)
	return nil
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
