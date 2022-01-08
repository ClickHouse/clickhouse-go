package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type (
	Float32 []float32
	Float64 []float64
	Int8    []int8
	Int16   []int16
	Int32   []int32
	Int64   []int64
	UInt8   []uint8
	UInt16  []uint16
	UInt32  []uint32
	UInt64  []uint64
)

var (
	_ Interface = (*Float32)(nil)
	_ Interface = (*Float64)(nil)
	_ Interface = (*Int8)(nil)
	_ Interface = (*Int16)(nil)
	_ Interface = (*Int32)(nil)
	_ Interface = (*Int64)(nil)
	_ Interface = (*UInt8)(nil)
	_ Interface = (*UInt16)(nil)
	_ Interface = (*UInt32)(nil)
	_ Interface = (*UInt64)(nil)
)

func (col *Float32) Rows() int {
	return len(*col)
}

func (col *Float32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *float32:
		*d = value[row]
	case **float32:
		*d = new(float32)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Float32) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Float32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case float32:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Float32) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Float32()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Float32) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Float32(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *Float64) Rows() int {
	return len(*col)
}

func (col *Float64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *float64:
		*d = value[row]
	case **float64:
		*d = new(float64)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Float64) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Float64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case float64:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Float64) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Float64()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Float64) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Float64(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *Int8) Rows() int {
	return len(*col)
}

func (col *Int8) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int8:
		*d = value[row]
	case **int8:
		*d = new(int8)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Int8) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Int8) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int8:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Int8) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Int8()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Int8) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Int8(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *Int16) Rows() int {
	return len(*col)
}

func (col *Int16) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int16:
		*d = value[row]
	case **int16:
		*d = new(int16)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Int16) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Int16) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int16:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Int16) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Int16()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Int16) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Int16(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *Int32) Rows() int {
	return len(*col)
}

func (col *Int32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int32:
		*d = value[row]
	case **int32:
		*d = new(int32)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Int32) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Int32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int32:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Int32) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Int32()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Int32) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Int32(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *Int64) Rows() int {
	return len(*col)
}

func (col *Int64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *int64:
		*d = value[row]
	case **int64:
		*d = new(int64)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *Int64) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *Int64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int64:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *Int64) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.Int64()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *Int64) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.Int64(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *UInt8) Rows() int {
	return len(*col)
}

func (col *UInt8) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint8:
		*d = value[row]
	case **uint8:
		*d = new(uint8)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *UInt8) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *UInt8) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint8:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *UInt8) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.UInt8()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *UInt8) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.UInt8(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *UInt16) Rows() int {
	return len(*col)
}

func (col *UInt16) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint16:
		*d = value[row]
	case **uint16:
		*d = new(uint16)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *UInt16) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *UInt16) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint16:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *UInt16) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.UInt16()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *UInt16) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.UInt16(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *UInt32) Rows() int {
	return len(*col)
}

func (col *UInt32) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint32:
		*d = value[row]
	case **uint32:
		*d = new(uint32)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *UInt32) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *UInt32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint32:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *UInt32) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.UInt32()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *UInt32) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.UInt32(v); err != nil {
			return err
		}
	}
	return nil
}

func (col *UInt64) Rows() int {
	return len(*col)
}

func (col *UInt64) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *uint64:
		*d = value[row]
	case **uint64:
		*d = new(uint64)
		**d = value[row]
	default:
		return fmt.Errorf("unsupported type %T", d)
	}
	return nil
}

func (col *UInt64) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *UInt64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uint64:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *UInt64) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.UInt64()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *UInt64) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.UInt64(v); err != nil {
			return err
		}
	}
	return nil
}
