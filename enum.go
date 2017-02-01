package clickhouse

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type enum8 enum
type enum16 enum

type enum struct {
	iv       map[string]interface{}
	vi       map[interface{}]string
	baseType interface{}
}

func (e *enum) toIdent(v interface{}) (string, error) {
	if ident, found := e.vi[v]; found {
		return ident, nil
	}
	return "", fmt.Errorf("invalid Enum value: %v", v)
}
func (e enum) toValue(ident string) (interface{}, error) {
	if value, found := e.iv[ident]; found {
		return value, nil
	}
	return "", fmt.Errorf("invalid Enum ident: %s", ident)
}

func parseEnum(str string) (enum, error) {
	var (
		data     string
		isEnum16 bool
	)
	if len(str) < 8 {
		return enum{}, fmt.Errorf("invalid Enum format: %s", str)
	}
	switch {
	case strings.HasPrefix(str, "Enum8"):
		data = str[6:]
	case strings.HasPrefix(str, "Enum16"):
		data = str[7:]
		isEnum16 = true
	default:
		return enum{}, fmt.Errorf("'%s' is not Enum type", str)
	}
	enum := enum{
		iv: make(map[string]interface{}),
		vi: make(map[interface{}]string),
	}
	for _, block := range strings.Split(data[:len(data)-1], ",") {
		parts := strings.Split(block, "=")
		if len(parts) != 2 {
			return enum, fmt.Errorf("invalid Enum format: %s", str)
		}
		var (
			ident      = strings.TrimSpace(parts[0])
			value, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 16)
		)
		if err != nil {
			return enum, fmt.Errorf("invalid Enum value: %v", err)
		}
		{
			var (
				ident             = ident[1 : len(ident)-1]
				value interface{} = int16(value)
			)
			if isEnum16 {
				enum.baseType = int16(0)
			} else {
				enum.baseType = int8(0)
				value = int8(value.(int16))
			}
			enum.iv[ident] = value
			enum.vi[value] = ident
		}
	}
	return enum, nil
}

func arrayStringToArrayEnum(arrLen uint64, data []byte, enum enum) ([]byte, error) {
	var (
		enumByffer  = wb(int(arrLen * 2))
		arrayBuffer = bytes.NewBuffer(data)
	)
	for i := 0; i < int(arrLen); i++ {
		ident, err := readString(arrayBuffer)
		if err != nil {
			return nil, err
		}
		value, err := enum.toValue(ident)
		if err != nil {
			return nil, err
		}
		if err := write(enumByffer, enum.baseType, value); err != nil {
			return nil, err
		}
	}
	return enumByffer.bytes(), nil
}
