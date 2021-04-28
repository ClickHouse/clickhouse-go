package column

import (
	"bytes"
	"encoding/binary"
	"testing"

	chbin "github.com/ClickHouse/clickhouse-go/lib/binary"
)

func TestDecimal_Write32(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []int32{
		0,
		1,
		-1,
		10,
		123,
		1234567,
		1234567890,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(5,3)",
			},
			nobits:    32,
			precision: 5,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int32(binary.LittleEndian.Uint32(buff.Bytes()))
		if value != attempt {
			t.Errorf("Expecting: %d; Got: %d", value, attempt)
		}
	}
}
