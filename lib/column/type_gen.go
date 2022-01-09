package column

import (
	"strings"
)

func (t Type) Column() (Interface, error) {
	if t.IsNullable() {
		base, err := t.Base().Column()
		if err != nil {
			return nil, err
		}
		return &Nullable{
			base: base,
		}, nil
	}
	switch t {
	case "Float32":
		return &Float32{}, nil
	case "Float64":
		return &Float64{}, nil
	case "Int8":
		return &Int8{}, nil
	case "Int16":
		return &Int16{}, nil
	case "Int32":
		return &Int32{}, nil
	case "Int64":
		return &Int64{}, nil
	case "UInt8":
		return &UInt8{}, nil
	case "UInt16":
		return &UInt16{}, nil
	case "UInt32":
		return &UInt32{}, nil
	case "UInt64":
		return &UInt64{}, nil
	case "String":
		return &String{}, nil
	case "DateTime":
		return &DateTime{}, nil
	}
	if strings.HasPrefix(string(t), "Enum") {
		return Enum(string(t))
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}
