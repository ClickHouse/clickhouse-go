package format

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// This file converts between native blocks and arrow record batches. It is
// shared by the Parquet and ArrowStream codecs; any format arrow-go can
// bridge (Feather, ORC, ...) can be built as a thin codec on top of it.

// unwrapCHType strips Nullable and LowCardinality wrappers from a ClickHouse
// type name.
func unwrapCHType(s string) (base string, nullable bool) {
	for {
		switch {
		case strings.HasPrefix(s, "Nullable(") && strings.HasSuffix(s, ")"):
			nullable = true
			s = s[len("Nullable(") : len(s)-1]
		case strings.HasPrefix(s, "LowCardinality(") && strings.HasSuffix(s, ")"):
			s = s[len("LowCardinality(") : len(s)-1]
		default:
			return s, nullable
		}
	}
}

func unsupportedTypeError(codecName string, t column.Type) error {
	return fmt.Errorf("type %s is not supported by the client-side %s codec; use the HTTP protocol where the server converts all types", t, codecName)
}

// arrowTypeFor maps a ClickHouse column type to an arrow type.
func arrowTypeFor(codecName string, t column.Type) (arrow.DataType, bool, error) {
	base, nullable := unwrapCHType(string(t))
	switch {
	case base == "Int8":
		return arrow.PrimitiveTypes.Int8, nullable, nil
	case base == "Int16":
		return arrow.PrimitiveTypes.Int16, nullable, nil
	case base == "Int32":
		return arrow.PrimitiveTypes.Int32, nullable, nil
	case base == "Int64":
		return arrow.PrimitiveTypes.Int64, nullable, nil
	case base == "UInt8":
		return arrow.PrimitiveTypes.Uint8, nullable, nil
	case base == "UInt16":
		return arrow.PrimitiveTypes.Uint16, nullable, nil
	case base == "UInt32":
		return arrow.PrimitiveTypes.Uint32, nullable, nil
	case base == "UInt64":
		return arrow.PrimitiveTypes.Uint64, nullable, nil
	case base == "Float32":
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case base == "Float64":
		return arrow.PrimitiveTypes.Float64, nullable, nil
	case base == "Bool":
		return arrow.FixedWidthTypes.Boolean, nullable, nil
	case base == "String", strings.HasPrefix(base, "FixedString("):
		return arrow.BinaryTypes.String, nullable, nil
	case base == "Date", base == "Date32":
		return arrow.FixedWidthTypes.Date32, nullable, nil
	case strings.HasPrefix(base, "DateTime64"):
		return &arrow.TimestampType{Unit: dateTime64ArrowUnit(base), TimeZone: "UTC"}, nullable, nil
	case strings.HasPrefix(base, "DateTime"):
		return &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, nullable, nil
	default:
		return nil, false, unsupportedTypeError(codecName, t)
	}
}

func dateTime64ArrowUnit(base string) arrow.TimeUnit {
	scale := 3
	if params := strings.TrimSuffix(strings.TrimPrefix(base, "DateTime64("), ")"); params != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(strings.Split(params, ",")[0])); err == nil {
			scale = n
		}
	}
	switch {
	case scale <= 0:
		return arrow.Second
	case scale <= 3:
		return arrow.Millisecond
	case scale <= 6:
		return arrow.Microsecond
	default:
		return arrow.Nanosecond
	}
}

// blockArrowSchema derives the arrow schema for a block's columns.
func blockArrowSchema(codecName string, block *proto.Block) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(block.Columns))
	for _, col := range block.Columns {
		dt, nullable, err := arrowTypeFor(codecName, col.Type())
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{Name: col.Name(), Type: dt, Nullable: nullable})
	}
	return arrow.NewSchema(fields, nil), nil
}

// buildRecordBatch appends every row of block to the builder and returns the
// resulting record batch. The caller owns the returned batch (Release it).
func buildRecordBatch(codecName string, builder *array.RecordBuilder, block *proto.Block) (arrow.RecordBatch, error) {
	rows := block.Rows()
	for i, col := range block.Columns {
		fb := builder.Field(i)
		for row := 0; row < rows; row++ {
			if err := appendToBuilder(codecName, fb, col, row); err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name(), err)
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

func appendToBuilder(codecName string, b array.Builder, col column.Interface, row int) error {
	v, isNull := rowValue(col, row)
	if isNull {
		b.AppendNull()
		return nil
	}
	var ok bool
	switch b := b.(type) {
	case *array.Int8Builder:
		var val int8
		if val, ok = v.(int8); ok {
			b.Append(val)
		}
	case *array.Int16Builder:
		var val int16
		if val, ok = v.(int16); ok {
			b.Append(val)
		}
	case *array.Int32Builder:
		var val int32
		if val, ok = v.(int32); ok {
			b.Append(val)
		}
	case *array.Int64Builder:
		var val int64
		if val, ok = v.(int64); ok {
			b.Append(val)
		}
	case *array.Uint8Builder:
		var val uint8
		if val, ok = v.(uint8); ok {
			b.Append(val)
		}
	case *array.Uint16Builder:
		var val uint16
		if val, ok = v.(uint16); ok {
			b.Append(val)
		}
	case *array.Uint32Builder:
		var val uint32
		if val, ok = v.(uint32); ok {
			b.Append(val)
		}
	case *array.Uint64Builder:
		var val uint64
		if val, ok = v.(uint64); ok {
			b.Append(val)
		}
	case *array.Float32Builder:
		var val float32
		if val, ok = v.(float32); ok {
			b.Append(val)
		}
	case *array.Float64Builder:
		var val float64
		if val, ok = v.(float64); ok {
			b.Append(val)
		}
	case *array.BooleanBuilder:
		var val bool
		if val, ok = v.(bool); ok {
			b.Append(val)
		}
	case *array.StringBuilder:
		var val string
		if val, ok = v.(string); ok {
			b.Append(val)
		}
	case *array.Date32Builder:
		var val time.Time
		if val, ok = v.(time.Time); ok {
			b.Append(arrow.Date32FromTime(val))
		}
	case *array.TimestampBuilder:
		var val time.Time
		if val, ok = v.(time.Time); ok {
			ts, err := arrow.TimestampFromTime(val, b.Type().(*arrow.TimestampType).Unit)
			if err != nil {
				return err
			}
			b.Append(ts)
		}
	default:
		return unsupportedTypeError(codecName, col.Type())
	}
	if !ok {
		return fmt.Errorf("cannot encode %T value as %s (%s codec)", v, b.Type(), codecName)
	}
	return nil
}

// recordStreamDecoder implements Decoder over any source of arrow record
// batches. next returns the caller-owned next batch or io.EOF.
type recordStreamDecoder struct {
	codecName string
	next      func() (arrow.RecordBatch, error)
	cur       arrow.RecordBatch
	offset    int
	colIdx    []int // block column i -> record column index
}

func (d *recordStreamDecoder) ReadBlock(block *proto.Block, maxRows int) (int, error) {
	appended := 0
	for appended < maxRows {
		if d.cur == nil || d.offset >= int(d.cur.NumRows()) {
			if d.cur != nil {
				d.cur.Release()
				d.cur = nil
			}
			rec, err := d.next()
			if err != nil {
				return appended, err
			}
			d.cur, d.offset = rec, 0
			if d.colIdx == nil {
				if err := d.buildColumnIndex(block); err != nil {
					return appended, err
				}
			}
		}
		n := int(d.cur.NumRows()) - d.offset
		if n > maxRows-appended {
			n = maxRows - appended
		}
		if err := d.appendRows(block, d.offset, n); err != nil {
			return appended, err
		}
		d.offset += n
		appended += n
	}
	return appended, nil
}

// buildColumnIndex matches record columns to block columns by name, the same
// convention the ClickHouse server uses for Parquet/Arrow input.
func (d *recordStreamDecoder) buildColumnIndex(block *proto.Block) error {
	schema := d.cur.Schema()
	d.colIdx = make([]int, len(block.Columns))
	for i, col := range block.Columns {
		indices := schema.FieldIndices(col.Name())
		if len(indices) == 0 {
			return fmt.Errorf("%s decode: input has no column %q (input columns: %v)", d.codecName, col.Name(), fieldNames(schema))
		}
		d.colIdx[i] = indices[0]
	}
	return nil
}

func fieldNames(schema *arrow.Schema) []string {
	names := make([]string, len(schema.Fields()))
	for i, f := range schema.Fields() {
		names[i] = f.Name
	}
	return names
}

func (d *recordStreamDecoder) appendRows(block *proto.Block, start, count int) error {
	for i, col := range block.Columns {
		arr := d.cur.Column(d.colIdx[i])
		for row := start; row < start+count; row++ {
			if err := appendArrowValue(d.codecName, col, arr, row); err != nil {
				return fmt.Errorf("%s decode: row %d: column %s: %w", d.codecName, row+1, col.Name(), err)
			}
		}
	}
	return nil
}

func appendArrowValue(codecName string, col column.Interface, arr arrow.Array, row int) error {
	if arr.IsNull(row) {
		return col.AppendRow(nil)
	}
	var v any
	switch arr := arr.(type) {
	case *array.Int8:
		v = arr.Value(row)
	case *array.Int16:
		v = arr.Value(row)
	case *array.Int32:
		v = arr.Value(row)
	case *array.Int64:
		v = arr.Value(row)
	case *array.Uint8:
		v = arr.Value(row)
	case *array.Uint16:
		v = arr.Value(row)
	case *array.Uint32:
		v = arr.Value(row)
	case *array.Uint64:
		v = arr.Value(row)
	case *array.Float32:
		v = arr.Value(row)
	case *array.Float64:
		v = arr.Value(row)
	case *array.Boolean:
		v = arr.Value(row)
	case *array.String:
		v = arr.Value(row)
	case *array.LargeString:
		v = arr.Value(row)
	case *array.Binary:
		v = string(arr.Value(row))
	case *array.Date32:
		v = arr.Value(row).ToTime()
	case *array.Date64:
		v = arr.Value(row).ToTime()
	case *array.Timestamp:
		v = arr.Value(row).ToTime(arr.DataType().(*arrow.TimestampType).Unit)
	default:
		return fmt.Errorf("arrow type %s is not supported by the client-side %s codec; use the HTTP protocol", arr.DataType(), codecName)
	}
	return appendConverted(col, v)
}

// appendConverted appends v to col, converting between numeric widths when the
// input schema does not exactly match the table schema (e.g. a Parquet int64
// column feeding a UInt8 table column).
func appendConverted(col column.Interface, v any) error {
	st := col.ScanType()
	if st == nil {
		return col.AppendRow(v)
	}
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}
	rv := reflect.ValueOf(v)
	if rv.Type() == st {
		return col.AppendRow(v)
	}
	if isNumericKind(rv.Kind()) && isNumericKind(st.Kind()) && rv.Type().ConvertibleTo(st) {
		return col.AppendRow(rv.Convert(st).Interface())
	}
	return col.AppendRow(v)
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}
