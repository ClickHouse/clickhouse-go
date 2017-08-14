package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type String struct {
	name, chType string
}

func (str *String) Name() string {
	return str.name
}

func (str *String) CHType() string {
	return str.chType
}

func (str *String) Read(decoder *binary.Decoder) (interface{}, error) {
	v, err := decoder.String()
	if err != nil {
		return "", err
	}
	return v, nil
}

func (str *String) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case string:
		return encoder.String(v)
	case []byte:
		return encoder.RawString(v)
	}
	return fmt.Errorf("unexpected type %T", v)
}

func (str *String) String() string {
	return fmt.Sprintf("%s (%s)", str.name, str.chType)
}
