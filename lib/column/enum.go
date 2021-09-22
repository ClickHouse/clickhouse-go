package column

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type Enum struct {
	iv map[string]interface{}
	vi map[interface{}]string
	base
	baseType interface{}
}

func (enum *Enum) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	var (
		err   error
		ident interface{}
	)
	switch enum.baseType.(type) {
	case int16:
		if ident, err = decoder.Int16(); err != nil {
			return nil, err
		}
	default:
		if ident, err = decoder.Int8(); err != nil {
			return nil, err
		}
	}
	if ident, found := enum.vi[ident]; found || isNull {
		return ident, nil
	}
	return nil, fmt.Errorf("invalid Enum value: %v", ident)
}

func (enum *Enum) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case string:
		return enum.encodeFromString(v, encoder)
	case uint8:
		if _, ok := enum.baseType.(int8); ok {
			return encoder.Int8(int8(v))
		}
	case int8:
		if _, ok := enum.baseType.(int8); ok {
			return encoder.Int8(v)
		}
	case uint16:
		if _, ok := enum.baseType.(int16); ok {
			return encoder.Int16(int16(v))
		}
	case int16:
		if _, ok := enum.baseType.(int16); ok {
			return encoder.Int16(v)
		}
	case int64:
		switch enum.baseType.(type) {
		case int8:
			return encoder.Int8(int8(v))
		case int16:
			return encoder.Int16(int16(v))
		}
	// nullable enums
	case *string:
		return enum.encodeFromString(*v, encoder)
	case *uint8:
		if _, ok := enum.baseType.(int8); ok {
			return encoder.Int8(int8(*v))
		}
	case *int8:
		if _, ok := enum.baseType.(int8); ok {
			return encoder.Int8(*v)
		}
	case *uint16:
		if _, ok := enum.baseType.(int16); ok {
			return encoder.Int16(int16(*v))
		}
	case *int16:
		if _, ok := enum.baseType.(int16); ok {
			return encoder.Int16(*v)
		}
	case *int64:
		switch enum.baseType.(type) {
		case int8:
			return encoder.Int8(int8(*v))
		case int16:
			return encoder.Int16(int16(*v))
		}
	}
	return &ErrUnexpectedType{
		T:      v,
		Column: enum,
	}
}

func (enum *Enum) encodeFromString(v string, encoder *binary.Encoder) error {
	ident, found := enum.iv[v]
	if !found {
		return fmt.Errorf("invalid Enum ident: %s", v)
	}
	switch ident := ident.(type) {
	case int8:
		return encoder.Int8(ident)
	case int16:
		return encoder.Int16(ident)
	default:
		return &ErrUnexpectedType{
			T:      ident,
			Column: enum,
		}
	}
}

func (enum *Enum) defaultValue() interface{} {
	return enum.baseType
}

func parseEnum(name, chType string) (*Enum, error) {
	if len(chType) < 8 {
		return nil, fmt.Errorf("invalid Enum format: %s", chType)
	}
	data, bits := "", 8
	switch {
	case strings.HasPrefix(chType, "Enum8"):
		data = chType[6:]
	case strings.HasPrefix(chType, "Enum16"):
		data = chType[7:]
		bits = 16
	default:
		return nil, fmt.Errorf("'%s' is not Enum type", chType)
	}
	enum := Enum{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: columnBaseTypes[""],
		},
		iv: make(map[string]interface{}),
		vi: make(map[interface{}]string),
	}
	for _, block := range strings.Split(data[:len(data)-1], ",") {
		parts := strings.Split(block, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Enum format: %s", chType)
		}
		value, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, bits)
		if err != nil {
			return nil, fmt.Errorf("invalid Enum value: %v", chType)
		}
		var val interface{}
		if bits == 8 {
			val = int8(value)
		} else {
			val = int16(value)
		}
		if enum.baseType == nil {
			enum.baseType = val
		}
		ident := strings.TrimSpace(parts[0])
		ident = ident[1 : len(ident)-1]
		enum.iv[ident] = val
		enum.vi[val] = ident
	}
	return &enum, nil
}
