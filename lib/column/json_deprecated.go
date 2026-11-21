package column

import (
	"encoding/binary"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

func (c *JSON) encodeObjectHeader_v1(buffer *proto.Buffer) error {
	buffer.PutUVarInt(uint64(c.maxDynamicPaths))
	buffer.PutUVarInt(uint64(c.totalDynamicPaths))

	for _, dynamicPath := range c.dynamicPaths {
		buffer.PutString(dynamicPath)
	}

	for i, col := range c.typedColumns {
		if serialize, ok := col.(CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(buffer); err != nil {
				return fmt.Errorf("failed to write prefix for typed path \"%s\" in json with type %s: %w", c.typedPaths[i], string(col.Type()), err)
			}
		}
	}

	for i, col := range c.dynamicColumns {
		err := col.WriteStatePrefix(buffer)
		if err != nil {
			return fmt.Errorf("failed to encode header for json dynamic path \"%s\": %w", c.dynamicPaths[i], err)
		}
	}

	return nil
}

func (c *JSON) encodeObjectData_v1(buffer *proto.Buffer) {
	for _, col := range c.typedColumns {
		col.Encode(buffer)
	}

	for _, col := range c.dynamicColumns {
		col.Encode(buffer)
	}

	// SharedData per row, empty for now.
	for i := 0; i < c.rows; i++ {
		buffer.PutUInt64(0)
	}
}

func (c *JSON) decodeObjectHeader_v1(reader *proto.Reader) error {
	maxDynamicPaths, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read max dynamic paths for json column: %w", err)
	}
	c.maxDynamicPaths = int(maxDynamicPaths)

	totalDynamicPaths, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read total dynamic paths for json column: %w", err)
	}
	c.totalDynamicPaths = int(totalDynamicPaths)

	c.dynamicPaths = make([]string, 0, totalDynamicPaths)
	for i := 0; i < int(totalDynamicPaths); i++ {
		dynamicPath, err := reader.Str()
		if err != nil {
			return fmt.Errorf("failed to read dynamic path name bytes at index %d for json column: %w", i, err)
		}

		c.dynamicPaths = append(c.dynamicPaths, dynamicPath)
		c.dynamicPathsIndex[dynamicPath] = len(c.dynamicPaths) - 1
	}

	for i, col := range c.typedColumns {
		if serialize, ok := col.(CustomSerialization); ok {
			if err := serialize.ReadStatePrefix(reader); err != nil {
				return fmt.Errorf("failed to read prefix for typed path \"%s\" with type %s in json: %w", c.typedPaths[i], string(col.Type()), err)
			}
		}
	}

	c.dynamicColumns = make([]*Dynamic, 0, totalDynamicPaths)
	for _, dynamicPath := range c.dynamicPaths {
		parsedColDynamic, _ := Type("Dynamic").Column("", c.sc)
		colDynamic := parsedColDynamic.(*Dynamic)

		err := colDynamic.ReadStatePrefix(reader)
		if err != nil {
			return fmt.Errorf("failed to decode dynamic header at path %s for json column: %w", dynamicPath, err)
		}

		c.dynamicColumns = append(c.dynamicColumns, colDynamic)
	}

	return nil
}

func (c *JSON) decodeObjectData_v1(reader *proto.Reader, rows int) error {
	for i, col := range c.typedColumns {
		typedPath := c.typedPaths[i]

		err := col.Decode(reader, rows)
		if err != nil {
			return fmt.Errorf("failed to decode %s typed path %s for json column: %w", col.Type(), typedPath, err)
		}
	}

	for i, col := range c.dynamicColumns {
		dynamicPath := c.dynamicPaths[i]

		err := col.Decode(reader, rows)
		if err != nil {
			return fmt.Errorf("failed to decode dynamic path %s for json column: %w", dynamicPath, err)
		}
	}

	if err := c.decodeSharedData_v1(reader, rows); err != nil {
		return err
	}

	return nil
}

func (c *JSON) decodeSharedData_v1(reader *proto.Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	if rows < 0 || rows > int(^uint(0)>>1)/8 {
		return fmt.Errorf("invalid json row count %d", rows)
	}
	offsets, err := reader.ReadRaw(8 * rows)
	if err != nil {
		return fmt.Errorf("failed to read shared data offsets for json column: %w", err)
	}
	var total uint64
	for i := 0; i < rows; i++ {
		offset := binary.LittleEndian.Uint64(offsets[8*i:])
		if offset < total {
			return fmt.Errorf("invalid shared data offsets for json column")
		}
		total = offset
	}
	if total == 0 {
		return nil
	}
	return unsupportedSharedDataError("JSON shared data")
}

func unsupportedSharedDataError(storage string) error {
	return fmt.Errorf("cannot decode %s: on ClickHouse 25.6 or later, enable output_format_native_use_flattened_dynamic_and_json_serialization=1; for readonly=1, an administrator must set it to 1 in the settings profile or mark it changeable_in_readonly; on earlier servers, upgrade to ClickHouse 25.6 or later, or read JSON as a string with output_format_native_write_json_as_string=1, or cast Dynamic values to a concrete type in the query", storage)
}
