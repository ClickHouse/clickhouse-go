package column

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type Enum8 struct {
	iv     map[string]uint8
	vi     map[uint8]string
	values []string
}

func (e *Enum8) Rows() int {
	return len(e.values)
}

func (e *Enum8) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < int(rows); i++ {
		v, err := decoder.UInt8()
		if err != nil {
			return err
		}
		e.values = append(e.values, e.vi[v])
	}
	return nil
}

func (e *Enum8) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.values[row]
	case **string:
		*d = new(string)
		**d = e.values[row]
	}
	return nil
}

func (e *Enum8) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case string:
		e.values = append(e.values, v)
	case null:
		e.values = append(e.values, "")
	}
	return nil
}

func (e *Enum8) Encode(encoder *binary.Encoder) error {
	for _, v := range e.values {
		if err := encoder.UInt8(e.iv[v]); err != nil {
			return err
		}
	}
	return nil
}

type Enum16 struct {
	iv     map[string]uint16
	vi     map[uint16]string
	values []string
}

func (e *Enum16) Rows() int {
	return len(e.values)
}

func (e *Enum16) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < int(rows); i++ {
		v, err := decoder.UInt16()
		if err != nil {
			return err
		}
		e.values = append(e.values, e.vi[v])
	}
	return nil
}

func (e *Enum16) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.values[row]
	case **string:
		*d = new(string)
		**d = e.values[row]
	}
	return nil
}

func (e *Enum16) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case string:
		e.values = append(e.values, v)
	case null:
		e.values = append(e.values, "")
	}
	return nil
}
func (e *Enum16) Encode(encoder *binary.Encoder) error {
	for _, v := range e.values {
		if err := encoder.UInt16(e.iv[v]); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ Interface = (*Enum8)(nil)
	_ Interface = (*Enum16)(nil)
)

func Enum(columnType string) (Interface, error) {
	var payload string
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
			iv: make(map[string]uint8, len(idents)),
			vi: make(map[uint8]string, len(idents)),
		}
		for i := range idents {
			enum.iv[idents[i]] = uint8(indexes[i])
			enum.vi[uint8(indexes[i])] = idents[i]
		}
		return &enum, nil
	}
	enum := Enum16{
		iv: make(map[string]uint16, len(idents)),
		vi: make(map[uint16]string, len(idents)),
	}
	for i := range idents {
		enum.iv[idents[i]] = uint16(indexes[i])
		enum.vi[uint16(indexes[i])] = idents[i]
	}
	return &enum, nil
}
