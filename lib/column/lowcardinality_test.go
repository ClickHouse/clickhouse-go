package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/stretchr/testify/assert"
)

func TestLow(t *testing.T) {
	var (
		buffer  bytes.Buffer
		decoder = binary.NewDecoder(&buffer)
		encoder = binary.NewEncoder(&buffer)
	)
	col := LowCardinality{
		index:  &String{},
		tmpIdx: make(map[interface{}]int),
	}
	for i := 0; i < 10; i++ {
		if err := col.AppendRow("HI"); !assert.NoError(t, err) {
			return
		}
	}
	if err := col.AppendRow("HI2"); !assert.NoError(t, err) {
		return
	}
	if err := col.AppendRow("HI3"); !assert.NoError(t, err) {
		return
	}
	if assert.NoError(t, col.Encode(encoder)) {
		encoder.Flush()
		{
			col2 := LowCardinality{
				index:  &String{},
				tmpIdx: make(map[interface{}]int),
			}
			if assert.NoError(t, col2.Decode(decoder, 0)) {
				assert.Equal(t, 12, col2.Rows())
				for i := 0; i < col.Rows(); i++ {
					switch {
					case i == 10:
						assert.Equal(t, "HI2", col2.Row(i, false))
					case i == 11:
						assert.Equal(t, "HI3", col2.Row(i, false))
					default:
						assert.Equal(t, "HI", col2.Row(i, false))
					}
				}
			}
		}
	}
}
