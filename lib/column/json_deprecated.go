package column

import (
	"bytes"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
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
	// ClickHouse stores overflow JSON paths as Array(Tuple(String, String)):
	// UInt64 offsets followed by the flattened Tuple columns. Reading only the
	// offsets leaves the Tuple payload in the stream and desyncs every later
	// column (issue #1854).
	col, err := Type("Array(Tuple(String, String))").Column("", c.sc)
	if err != nil {
		return fmt.Errorf("failed to init shared data for json column: %w", err)
	}
	if err := col.Decode(reader, rows); err != nil {
		return fmt.Errorf("failed to read shared data for json column: %w", err)
	}
	c.sharedData = col
	return nil
}

func (c *JSON) mergeSharedDataRow(obj *chcol.JSON, row int) {
	if c.sharedData == nil || row >= c.sharedData.Rows() {
		return
	}
	for _, pair := range sharedDataPairs(c.sharedData.Row(row, false)) {
		value, chType := decodeSharedDataValue(pair[1])
		obj.SetValueAtPath(pair[0], chcol.NewDynamicWithType(value, chType))
	}
}

func sharedDataPairs(raw any) [][2]string {
	switch rows := raw.(type) {
	case [][]any:
		out := make([][2]string, 0, len(rows))
		for _, p := range rows {
			if len(p) < 2 {
				continue
			}
			path, _ := p[0].(string)
			val, _ := p[1].(string)
			if path != "" {
				out = append(out, [2]string{path, val})
			}
		}
		return out
	case []any:
		out := make([][2]string, 0, len(rows))
		for _, item := range rows {
			p, ok := item.([]any)
			if !ok || len(p) < 2 {
				continue
			}
			path, _ := p[0].(string)
			val, _ := p[1].(string)
			if path != "" {
				out = append(out, [2]string{path, val})
			}
		}
		return out
	default:
		return nil
	}
}

// Binary type indexes from ClickHouse src/DataTypes/DataTypesBinaryEncoding.cpp.
const (
	binNothing  = 0x00
	binUInt8    = 0x01
	binUInt16   = 0x02
	binUInt32   = 0x03
	binUInt64   = 0x04
	binInt8     = 0x07
	binInt16    = 0x08
	binInt32    = 0x09
	binInt64    = 0x0A
	binFloat32  = 0x0D
	binFloat64  = 0x0E
	binString   = 0x15
	binArray    = 0x1E
	binNullable = 0x23
	binBool     = 0x2D
	binDynamic  = 0x2B
)

type sharedBinType struct {
	kind  byte
	child *sharedBinType
	name  string
}

func decodeSharedDataValue(raw string) (any, string) {
	if raw == "" {
		return nil, ""
	}
	r := proto.NewReader(bytes.NewReader([]byte(raw)))
	t, err := readSharedBinType(r)
	if err != nil {
		return raw, "String"
	}
	v, err := readSharedBinValue(r, t)
	if err != nil {
		return raw, "String"
	}
	return v, t.name
}

func readSharedBinType(r *proto.Reader) (sharedBinType, error) {
	idx, err := r.ReadByte()
	if err != nil {
		return sharedBinType{}, err
	}
	switch idx {
	case binNothing:
		return sharedBinType{kind: idx, name: "Nothing"}, nil
	case binUInt8:
		return sharedBinType{kind: idx, name: "UInt8"}, nil
	case binUInt16:
		return sharedBinType{kind: idx, name: "UInt16"}, nil
	case binUInt32:
		return sharedBinType{kind: idx, name: "UInt32"}, nil
	case binUInt64:
		return sharedBinType{kind: idx, name: "UInt64"}, nil
	case binInt8:
		return sharedBinType{kind: idx, name: "Int8"}, nil
	case binInt16:
		return sharedBinType{kind: idx, name: "Int16"}, nil
	case binInt32:
		return sharedBinType{kind: idx, name: "Int32"}, nil
	case binInt64:
		return sharedBinType{kind: idx, name: "Int64"}, nil
	case binFloat32:
		return sharedBinType{kind: idx, name: "Float32"}, nil
	case binFloat64:
		return sharedBinType{kind: idx, name: "Float64"}, nil
	case binString:
		return sharedBinType{kind: idx, name: "String"}, nil
	case binBool:
		return sharedBinType{kind: idx, name: "Bool"}, nil
	case binArray:
		child, err := readSharedBinType(r)
		if err != nil {
			return sharedBinType{}, err
		}
		return sharedBinType{kind: idx, child: &child, name: "Array(" + child.name + ")"}, nil
	case binNullable:
		child, err := readSharedBinType(r)
		if err != nil {
			return sharedBinType{}, err
		}
		return sharedBinType{kind: idx, child: &child, name: "Nullable(" + child.name + ")"}, nil
	case binDynamic:
		if _, err := r.ReadByte(); err != nil { // uint8 max_types
			return sharedBinType{}, err
		}
		return sharedBinType{kind: idx, name: "Dynamic"}, nil
	default:
		return sharedBinType{}, fmt.Errorf("unsupported shared-data type index 0x%02x", idx)
	}
}

func readSharedBinValue(r *proto.Reader, t sharedBinType) (any, error) {
	switch t.kind {
	case binNothing:
		return nil, nil
	case binUInt8:
		return r.UInt8()
	case binUInt16:
		return r.UInt16()
	case binUInt32:
		return r.UInt32()
	case binUInt64:
		return r.UInt64()
	case binInt8:
		return r.Int8()
	case binInt16:
		return r.Int16()
	case binInt32:
		return r.Int32()
	case binInt64:
		return r.Int64()
	case binFloat32:
		return r.Float32()
	case binFloat64:
		return r.Float64()
	case binString:
		return r.Str()
	case binBool:
		return r.Bool()
	case binArray:
		n, err := r.UVarInt()
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, n)
		for i := uint64(0); i < n; i++ {
			v, err := readSharedBinValue(r, *t.child)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case binNullable:
		flag, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if flag != 0 {
			return nil, nil
		}
		return readSharedBinValue(r, *t.child)
	case binDynamic:
		inner, err := readSharedBinType(r)
		if err != nil {
			return nil, err
		}
		return readSharedBinValue(r, inner)
	default:
		return nil, fmt.Errorf("unsupported shared-data type index 0x%02x", t.kind)
	}
}
