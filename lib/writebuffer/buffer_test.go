package writebuffer

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/lib/leakypool"
	"github.com/stretchr/testify/assert"
)

func Test_WriteBuffer_SafeWithLeakyPool(t *testing.T) {
	leakypool.InitBytePool(1)
	wb := New(InitialSize)

	n, err := wb.Write(make([]byte, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	leakypool.PutBytes(make([]byte, InitialSize))

	assert.NotPanics(t, func() {
		n, err = wb.Write(make([]byte, InitialSize+1))
		assert.Equal(t, InitialSize+1, n)
		assert.NoError(t, err)
	})
}
