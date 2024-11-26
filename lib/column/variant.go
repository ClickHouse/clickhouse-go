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
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

const NullVariantDiscriminator uint8 = 255

type ColVariant struct {
	chType Type
	name   string
	rows   int

	serializationVersion uint64

	discriminators []uint8
	offsets        []int
	lengthsByType  map[uint8]int

	columns []Interface
	index   map[string]int
}

func (c *ColVariant) parse(t Type, tz *time.Location) (_ Interface, err error) {
	c.chType = t
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
	c.index = make(map[string]int)

	for i, ct := range elements {
		column, err := ct.colType.Column(ct.name, tz)
		if err != nil {
			return nil, err
		}

		c.columns = append(c.columns, column)
		c.index[ct.name] = i
	}

	if len(c.columns) != 0 {
		return c, nil
	}

	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (c *ColVariant) Name() string {
	return c.name
}

func (c *ColVariant) Type() Type {
	return c.chType
}

func (c *ColVariant) Rows() int {
	return c.rows
}

func (c *ColVariant) Row(i int, ptr bool) any {
	typeIndex := c.discriminators[i]
	if typeIndex == NullVariantDiscriminator {
		return nil
	}

	return c.columns[typeIndex].Row(c.offsets[i], ptr)
}

func (c *ColVariant) ScanRow(dest any, row int) error {
	typeIndex := c.discriminators[row]
	offsetIndex := c.offsets[row]
	var value any
	if typeIndex != NullVariantDiscriminator {
		value = c.columns[typeIndex].Row(offsetIndex, false)
	}

	switch v := dest.(type) {
	case *chcol.Variant:
		vt := chcol.NewVariant(value)
		*v = vt
	case **chcol.Variant:
		vt := chcol.NewVariant(value)
		**v = vt
	default:
		if typeIndex == NullVariantDiscriminator {
			return nil
		}

		if err := c.columns[typeIndex].ScanRow(dest, offsetIndex); err != nil {
			return err
		}
	}

	return nil
}

func (c *ColVariant) Append(v any) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *ColVariant) AppendRow(v any) error {
	if v == nil {
		c.rows++
		c.discriminators = append(c.discriminators, NullVariantDiscriminator)
		return nil
	}

	var forcedType Type
	switch v.(type) {
	case chcol.VariantWithType:
		forcedType = Type(v.(chcol.VariantWithType).Type())
	case *chcol.VariantWithType:
		forcedType = Type(v.(*chcol.VariantWithType).Type())
	}

	if forcedType != "" {
		var i int
		var col Interface
		var ok bool
		// TODO: this could be pre-calculated as a map[string]int (name->index)
		for i, col = range c.columns {
			if col.Type() == forcedType {
				ok = true
				break
			}
		}

		if !ok {
			return fmt.Errorf("value %v cannot be stored in variant column %s %s with forced type %s: type not present in variant", v, c.name, c.chType, forcedType)
		}

		if err := col.AppendRow(v); err != nil {
			return fmt.Errorf("value %v cannot be stored in variant column %s %s with forced type %s: %w", v, c.name, c.chType, forcedType, err)
		}

		c.rows++
		c.discriminators = append(c.discriminators, uint8(i))
		return nil
	}

	// If preferred type wasn't provided, try each column
	var err error
	for i, col := range c.columns {
		if err = col.AppendRow(v); err == nil {
			c.rows++
			c.discriminators = append(c.discriminators, uint8(i))
			return nil
		}
	}

	return fmt.Errorf("value %v cannot be stored in variant column %s %s: %w", v, c.name, c.chType, err)
}

func (c *ColVariant) Encode(buffer *proto.Buffer) {
	buffer.PutUInt64(c.serializationVersion)
	buffer.PutRaw(c.discriminators)

	for _, col := range c.columns {
		col.Encode(buffer)
	}
}

func (c *ColVariant) ScanType() reflect.Type {
	//TODO implement me
	panic("implement me")
}

func (c *ColVariant) Reset() {
	//TODO implement me
	panic("implement me")
}

func (c *ColVariant) Decode(reader *proto.Reader, rows int) error {
	c.rows = rows
	var err error
	c.serializationVersion, err = reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read variant serialization version: %w", err)
	}

	c.discriminators = make([]uint8, c.rows)
	c.offsets = make([]int, c.rows)
	c.lengthsByType = make(map[uint8]int, len(c.columns))

	for i := 0; i < c.rows; i++ {
		disc, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read variant discriminator at index %d: %w", i, err)
		}

		c.discriminators[i] = disc
		if c.lengthsByType[disc] == 0 {
			c.lengthsByType[disc] = 1
		} else {
			c.lengthsByType[disc]++
		}

		c.offsets[i] = c.lengthsByType[disc] - 1
	}

	for i, col := range c.columns {
		cRows := c.lengthsByType[uint8(i)]
		if err := col.Decode(reader, cRows); err != nil {
			return fmt.Errorf("failed to decode variant column with %s type: %w", col.Type(), err)
		}
	}

	return nil
}
