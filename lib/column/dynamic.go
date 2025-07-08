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
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

const SupportedDynamicSerializationVersion = 3
const DeprecatedSupportedDynamicSerializationVersion = 1
const DefaultMaxDynamicTypes = 32
const DynamicNullDiscriminator = -1 // The Null index changes as data is being built, use -1 as placeholder for writes.

type Dynamic struct {
	chType Type
	tz     *time.Location
	name   string

	totalTypes     int // Null is last type index + 1, so this doubles as the Null type index for reads.
	discriminators []int
	offsets        []int

	columns           []Interface
	columnIndexByName map[string]int
}

func (c *Dynamic) parse(t Type, tz *time.Location) (_ *Dynamic, err error) {
	c.chType = t
	c.tz = tz
	tStr := string(t)

	c.columnIndexByName = make(map[string]int)

	if tStr == "Dynamic" {
		return c, nil
	}

	if !strings.HasPrefix(tStr, "Dynamic(") || !strings.HasSuffix(tStr, ")") {
		return nil, &UnsupportedColumnTypeError{t: t}
	}

	return c, nil
}

func (c *Dynamic) addColumn(col Interface) int {
	colIndex := len(c.columns)
	c.columns = append(c.columns, col)
	c.columnIndexByName[string(col.Type())] = colIndex
	c.totalTypes++

	return colIndex
}

func (c *Dynamic) Name() string {
	return c.name
}

func (c *Dynamic) Type() Type {
	return c.chType
}

func (c *Dynamic) Rows() int {
	return len(c.discriminators)
}

func (c *Dynamic) Row(i int, ptr bool) any {
	typeIndex := c.discriminators[i]
	offsetIndex := c.offsets[i]
	var value any
	var chType string
	if typeIndex != c.totalTypes {
		value = c.columns[typeIndex].Row(offsetIndex, ptr)
		chType = string(c.columns[typeIndex].Type())
	}

	dyn := chcol.NewDynamicWithType(value, chType)
	if ptr {
		return &dyn
	}

	return dyn
}

func (c *Dynamic) ScanRow(dest any, row int) error {
	typeIndex := c.discriminators[row]
	offsetIndex := c.offsets[row]
	var value any
	var chType string
	if typeIndex != c.totalTypes {
		value = c.columns[typeIndex].Row(offsetIndex, false)
		chType = string(c.columns[typeIndex].Type())
	}

	switch v := dest.(type) {
	case *chcol.Dynamic:
		dyn := chcol.NewDynamicWithType(value, chType)
		*v = dyn
	case **chcol.Dynamic:
		dyn := chcol.NewDynamicWithType(value, chType)
		**v = dyn
	default:
		if typeIndex == c.totalTypes {
			return nil
		}

		if err := c.columns[typeIndex].ScanRow(dest, offsetIndex); err != nil {
			return err
		}
	}

	return nil
}

func (c *Dynamic) appendDiscriminatorRow(d int) {
	c.discriminators = append(c.discriminators, d)
}

func (c *Dynamic) appendNullRow() {
	c.appendDiscriminatorRow(DynamicNullDiscriminator)
}

func (c *Dynamic) Append(v any) (nulls []uint8, err error) {
	switch vv := v.(type) {
	case []chcol.Dynamic:
		for i, dyn := range vv {
			err := c.AppendRow(dyn)
			if err != nil {
				return nil, fmt.Errorf("failed to AppendRow at index %d: %w", i, err)
			}
		}

		return nil, nil
	case []*chcol.Dynamic:
		for i, dyn := range vv {
			err := c.AppendRow(dyn)
			if err != nil {
				return nil, fmt.Errorf("failed to AppendRow at index %d: %w", i, err)
			}
		}

		return nil, nil
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   string(c.chType),
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}

			return c.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   string(c.chType),
			From: fmt.Sprintf("%T", v),
		}
	}
}

func (c *Dynamic) AppendRow(v any) error {
	var requestedType string
	switch vv := v.(type) {
	case nil:
		c.appendNullRow()
		return nil
	case chcol.Dynamic:
		requestedType = vv.Type()
		v = vv.Any()
		if vv.Nil() {
			c.appendNullRow()
			return nil
		}
	case *chcol.Dynamic:
		requestedType = vv.Type()
		v = vv.Any()
		if vv.Nil() {
			c.appendNullRow()
			return nil
		}
	}

	if requestedType != "" {
		var col Interface
		colIndex, ok := c.columnIndexByName[requestedType]
		if ok {
			col = c.columns[colIndex]
		} else {
			newCol, err := Type(requestedType).Column("", c.tz)
			if err != nil {
				return fmt.Errorf("value \"%v\" cannot be stored in dynamic column %s with requested type %s: unable to append type: %w", v, c.chType, requestedType, err)
			}

			colIndex = c.addColumn(newCol)
			col = newCol
		}

		if err := col.AppendRow(v); err != nil {
			return fmt.Errorf("value \"%v\" cannot be stored in dynamic column %s with requested type %s: %w", v, c.chType, requestedType, err)
		}

		c.appendDiscriminatorRow(colIndex)
		return nil
	}

	// If preferred type wasn't provided, try each column
	for i, col := range c.columns {
		if err := col.AppendRow(v); err == nil {
			c.appendDiscriminatorRow(i)
			return nil
		}
	}

	// If no existing columns match, try matching a ClickHouse type from common Go types
	inferredTypeName := inferClickHouseTypeFromGoType(v)
	if inferredTypeName != "" {
		return c.AppendRow(chcol.NewDynamicWithType(v, inferredTypeName))
	}

	return fmt.Errorf("value \"%v\" cannot be stored in dynamic column: no compatible types. hint: use clickhouse.DynamicWithType to wrap the value", v)
}

func (c *Dynamic) encodeHeader(buffer *proto.Buffer) error {
	buffer.PutUInt64(SupportedDynamicSerializationVersion)
	buffer.PutUVarInt(uint64(c.totalTypes))

	for _, col := range c.columns {
		buffer.PutString(string(col.Type()))
	}

	for _, col := range c.columns {
		if serialize, ok := col.(CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(buffer); err != nil {
				return fmt.Errorf("failed to write prefix for type %s in dynamic: %w", string(col.Type()), err)
			}
		}
	}

	return nil
}

func discriminatorWriter(totalTypes int, buffer *proto.Buffer) func(int) {
	switch {
	case totalTypes <= math.MaxUint8:
		return func(d int) { buffer.PutUInt8(uint8(d)) }
	case totalTypes <= math.MaxUint16:
		return func(d int) { buffer.PutUInt16(uint16(d)) }
	case totalTypes <= math.MaxUint32:
		return func(d int) { buffer.PutUInt32(uint32(d)) }
	default:
		return func(d int) { buffer.PutUInt64(uint64(d)) }
	}
}

func (c *Dynamic) encodeData(buffer *proto.Buffer) {
	writeDiscriminator := discriminatorWriter(c.totalTypes, buffer)
	for _, typeIndex := range c.discriminators {
		if typeIndex == DynamicNullDiscriminator {
			typeIndex = c.totalTypes
		}

		writeDiscriminator(typeIndex)
	}

	for _, col := range c.columns {
		col.Encode(buffer)
	}
}

func (c *Dynamic) WriteStatePrefix(buffer *proto.Buffer) error {
	return c.encodeHeader(buffer)
}

func (c *Dynamic) Encode(buffer *proto.Buffer) {
	c.encodeData(buffer)
}

func (c *Dynamic) ScanType() reflect.Type {
	return scanTypeDynamic
}

func (c *Dynamic) Reset() {
	c.discriminators = c.discriminators[:0]

	for _, col := range c.columns {
		col.Reset()
	}
}

func (c *Dynamic) decodeHeader(reader *proto.Reader) error {
	dynamicSerializationVersion, err := reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read dynamic serialization version: %w", err)
	}

	if dynamicSerializationVersion == DeprecatedSupportedDynamicSerializationVersion {
		return fmt.Errorf("deprecated dynamic serialization version: %d, enable \"output_format_native_use_flattened_dynamic_and_json_serialization\" in your settings", dynamicSerializationVersion)
	} else if dynamicSerializationVersion != SupportedDynamicSerializationVersion {
		return fmt.Errorf("unsupported dynamic serialization version: %d", dynamicSerializationVersion)
	}

	totalTypes, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read total types for dynamic column: %w", err)
	}

	c.columns = make([]Interface, 0, totalTypes)
	c.columnIndexByName = make(map[string]int, totalTypes)
	for i := uint64(0); i < totalTypes; i++ {
		typeName, err := reader.Str()
		if err != nil {
			return fmt.Errorf("failed to read type name at index %d for dynamic column: %w", i, err)
		}

		col, err := Type(typeName).Column("", c.tz)
		if err != nil {
			return fmt.Errorf("failed to add dynamic column with type %s: %w", typeName, err)
		}

		c.addColumn(col)
	}

	for _, col := range c.columns {
		if serialize, ok := col.(CustomSerialization); ok {
			if err := serialize.ReadStatePrefix(reader); err != nil {
				return fmt.Errorf("failed to read prefix for type %s in dynamic: %w", col.Type(), err)
			}
		}
	}

	return nil
}

func discriminatorReader(totalTypes int, reader *proto.Reader) func() (int, error) {
	switch {
	case totalTypes <= math.MaxUint8:
		return func() (int, error) {
			v, err := reader.UInt8()
			return int(v), err
		}
	case totalTypes <= math.MaxUint16:
		return func() (int, error) {
			v, err := reader.UInt16()
			return int(v), err
		}
	case totalTypes <= math.MaxUint32:
		return func() (int, error) {
			v, err := reader.UInt32()
			return int(v), err
		}
	default:
		return func() (int, error) {
			v, err := reader.UInt64()
			return int(v), err
		}
	}
}

func (c *Dynamic) decodeData(reader *proto.Reader, rows int) error {
	c.discriminators = make([]int, rows)
	c.offsets = make([]int, rows)
	rowCountByType := make([]int, len(c.columns))

	readDiscriminator := discriminatorReader(c.totalTypes, reader)
	for i := 0; i < rows; i++ {
		disc, err := readDiscriminator()
		if err != nil {
			return fmt.Errorf("failed to read discriminator at index %d: %w", i, err)
		}

		c.discriminators[i] = disc
		if disc != c.totalTypes {
			c.offsets[i] = rowCountByType[disc]
			rowCountByType[disc]++
		}
	}

	for i, col := range c.columns {
		cRows := rowCountByType[i]
		if err := col.Decode(reader, cRows); err != nil {
			return fmt.Errorf("failed to decode dynamic column with %s type: %w", col.Type(), err)
		}
	}

	return nil
}

func (c *Dynamic) ReadStatePrefix(reader *proto.Reader) error {
	err := c.decodeHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to decode dynamic header: %w", err)
	}

	return nil
}

func (c *Dynamic) Decode(reader *proto.Reader, rows int) error {
	err := c.decodeData(reader, rows)
	if err != nil {
		return fmt.Errorf("failed to decode dynamic data: %w", err)
	}

	return nil
}
