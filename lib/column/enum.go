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
	"math"
	"strconv"
	"strings"
)

func Enum(chType Type, name string) (Interface, error) {
	var (
		payload    string
		columnType = string(chType)
	)
	if len(columnType) < 8 {
		return nil, &Error{
			ColumnType: string(chType),
			Err:        errors.New("invalid Enum"),
		}
	}
	switch {
	case strings.HasPrefix(columnType, "Enum8"):
		payload = columnType[6:]
	case strings.HasPrefix(columnType, "Enum16"):
		payload = columnType[7:]
	default:
		return nil, &Error{
			ColumnType: string(chType),
			Err:        errors.New("invalid Enum"),
		}
	}
	var (
		idents  []string
		indexes []int64
	)
	for _, block := range strings.Split(payload[:len(payload)-1], ",") {
		parts := strings.Split(block, "=")
		if len(parts) != 2 {
			return nil, &Error{
				ColumnType: string(chType),
				Err:        errors.New("invalid Enum"),
			}
		}
		var (
			ident      = strings.TrimSpace(parts[0])
			index, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 16)
		)
		if err != nil || len(ident) < 2 {
			return nil, &Error{
				ColumnType: string(chType),
				Err:        errors.New("invalid Enum"),
			}
		}
		ident = ident[1 : len(ident)-1]
		idents, indexes = append(idents, ident), append(indexes, index)
	}
	if strings.HasPrefix(columnType, "Enum8") {
		enum := Enum8{
			iv:     make(map[string]uint8, len(idents)),
			vi:     make(map[uint8]string, len(idents)),
			chType: chType,
			name:   name,
		}
		for i := range idents {
			if indexes[i] > math.MaxUint8 {
				return nil, &Error{
					ColumnType: string(chType),
					Err:        errors.New("invalid Enum"),
				}
			}
			enum.iv[idents[i]] = uint8(indexes[i])
			enum.vi[uint8(indexes[i])] = idents[i]
		}
		return &enum, nil
	}
	enum := Enum16{
		iv:     make(map[string]uint16, len(idents)),
		vi:     make(map[uint16]string, len(idents)),
		chType: chType,
		name:   name,
	}
	for i := range idents {
		enum.iv[idents[i]] = uint16(indexes[i])
		enum.vi[uint16(indexes[i])] = idents[i]
	}
	return &enum, nil
}
