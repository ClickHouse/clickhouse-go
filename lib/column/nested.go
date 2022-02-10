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
	"strings"
)

type Nested struct {
	Interface
}

func (col *Nested) parse(t Type) (_ Interface, err error) {
	columns := fmt.Sprintf("Array(Tuple(%s))", strings.Join(nestedColumns(t.params()), ", "))
	if col.Interface, err = (&Array{}).parse(Type(columns)); err != nil {
		return nil, err
	}
	return col, nil
}

func nestedColumns(raw string) (columns []string) {
	var (
		begin    int
		brackets int
	)
	for i, r := range raw + "," {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ' ':
			if brackets == 0 {
				begin = i + 1
			}
		case ',':
			if brackets == 0 {
				columns, begin = append(columns, raw[begin:i]), i+1
				continue
			}
		}
	}
	for i, column := range columns {
		if strings.HasPrefix(column, "Nested(") {
			columns[i] = fmt.Sprintf("Array(Tuple(%s))", strings.Join(nestedColumns(Type(column).params()), ", "))
		}
	}
	return
}

var _ Interface = (*Nested)(nil)
