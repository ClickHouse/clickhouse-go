package column

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/binary"
)

type String struct{ base }

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
