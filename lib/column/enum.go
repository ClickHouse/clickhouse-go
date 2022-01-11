package column

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Enum8 struct {
	iv     map[string]uint8
	vi     map[uint8]string
	chType Type
	values UInt8
}

func (e *Enum8) Type() Type {
	return e.chType
}

func (e *Enum8) Rows() int {
	return len(e.values)
}

func (e *Enum8) RowValue(row int) interface{} {
	return e.vi[e.values[row]]
}

func (e *Enum8) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.vi[e.values[row]]
	case **string:
		*d = new(string)
		**d = e.vi[e.values[row]]
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Enum8",
		}
	}
	return nil
}

func (e *Enum8) Append(v interface{}) error {
	switch v := v.(type) {
	case []string:
		for _, elem := range v {
			v, ok := e.iv[elem]
			if !ok {
				return &UnknownElementForEnum{
					element: elem,
				}
			}
			e.values = append(e.values, v)
		}
	default:
		return &ColumnConverterErr{
			op:   "Append",
			to:   "Enum8",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (e *Enum8) AppendRow(elem interface{}) error {
	switch elem := elem.(type) {
	case string:
		v, ok := e.iv[elem]
		if !ok {
			return &UnknownElementForEnum{
				element: elem,
			}
		}

		e.values = append(e.values, v)
	case null:
		e.values = append(e.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Enum8",
			from: fmt.Sprintf("%T", elem),
		}
	}
	return nil
}

func (e *Enum8) Decode(decoder *binary.Decoder, rows int) error {
	return e.values.Decode(decoder, rows)
}

func (e *Enum8) Encode(encoder *binary.Encoder) error {
	return e.values.Encode(encoder)
}

type Enum16 struct {
	iv     map[string]uint16
	vi     map[uint16]string
	chType Type
	values UInt16
}

func (e *Enum16) Type() Type {
	return e.chType
}

func (e *Enum16) Rows() int {
	return len(e.values)
}

func (e *Enum16) RowValue(row int) interface{} {
	return e.vi[e.values[row]]
}

func (e *Enum16) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.vi[e.values[row]]
	case **string:
		*d = new(string)
		**d = e.vi[e.values[row]]
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Enum16",
		}
	}
	return nil
}

func (e *Enum16) Append(v interface{}) error {
	switch v := v.(type) {
	case []string:
		for _, elem := range v {
			v, ok := e.iv[elem]
			if !ok {
				return &UnknownElementForEnum{
					element: elem,
				}
			}
			e.values = append(e.values, v)
		}
	default:
		return &ColumnConverterErr{
			op:   "Append",
			to:   "Enum16",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (e *Enum16) AppendRow(elem interface{}) error {
	switch elem := elem.(type) {
	case string:
		v, ok := e.iv[elem]
		if !ok {
			return &UnknownElementForEnum{
				element: elem,
			}
		}
		e.values = append(e.values, v)
	case null:
		e.values = append(e.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Enum16",
			from: fmt.Sprintf("%T", elem),
		}
	}
	return nil
}

func (e *Enum16) Decode(decoder *binary.Decoder, rows int) error {
	return e.values.Decode(decoder, rows)
}

func (e *Enum16) Encode(encoder *binary.Encoder) error {
	return e.values.Encode(encoder)
}

var (
	_ Interface = (*Enum8)(nil)
	_ Interface = (*Enum16)(nil)
)

func Enum(chType Type) (Interface, error) {
	var (
		payload    string
		columnType = string(chType)
	)
	if len(columnType) < 8 {
		return nil, fmt.Errorf("invalid Enum format: %s", columnType)
	}
	switch {
	case strings.HasPrefix(columnType, "Enum8"):
		payload = columnType[6:]
	case strings.HasPrefix(columnType, "Enum16"):
		payload = columnType[7:]
	default:
		return nil, fmt.Errorf("'%s' is not Enum type", columnType)
	}
	var (
		idents  []string
		indexes []int64
	)
	for _, block := range strings.Split(payload[:len(payload)-1], ",") {
		parts := strings.Split(block, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Enum format: %s", columnType)
		}
		var (
			ident      = strings.TrimSpace(parts[0])
			index, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 16)
		)
		if err != nil || len(ident) < 2 {
			return nil, fmt.Errorf("invalid Enum value: %v", columnType)
		}
		ident = ident[1 : len(ident)-1]
		idents, indexes = append(idents, ident), append(indexes, index)
	}
	if strings.HasPrefix(columnType, "Enum8") {
		enum := Enum8{
			iv:     make(map[string]uint8, len(idents)),
			vi:     make(map[uint8]string, len(idents)),
			chType: chType,
		}
		for i := range idents {
			enum.iv[idents[i]] = uint8(indexes[i])
			enum.vi[uint8(indexes[i])] = idents[i]
		}
		return &enum, nil
	}
	enum := Enum16{
		iv:     make(map[string]uint16, len(idents)),
		vi:     make(map[uint16]string, len(idents)),
		chType: chType,
	}
	for i := range idents {
		enum.iv[idents[i]] = uint16(indexes[i])
		enum.vi[uint16(indexes[i])] = idents[i]
	}
	return &enum, nil
}
