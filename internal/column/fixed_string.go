package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type FixedString struct {
	name, chType string
	len          int
}

func (str *FixedString) Name() string {
	return str.name
}

func (str *FixedString) CHType() string {
	return str.chType
}

func (str *FixedString) Read(decoder *binary.Decoder) (interface{}, error) {
	v, err := decoder.Fixed(str.len)
	if err != nil {
		return "", err
	}
	return v, nil
}

func (str *FixedString) Write(encoder *binary.Encoder, v interface{}) error {
	var fixedString []byte
	switch v := v.(type) {
	case string:
		fixedString = binary.Str2Bytes(v)
	case []byte:
		fixedString = v
	default:
		return fmt.Errorf("unexpected type %T", v)
	}
	switch {
	case len(fixedString) > str.len:
		return fmt.Errorf("too large value '%s' (expected %d, got %d)", fixedString, str.len, len(fixedString))
	case len(fixedString) < str.len:
		tmp := make([]byte, str.len)
		copy(tmp, fixedString)
		fixedString = tmp
	}
	if _, err := encoder.Write(fixedString); err != nil {
		return err
	}
	return nil
}

func (str *FixedString) String() string {
	return fmt.Sprintf("%s (%s)", str.name, str.chType)
}
