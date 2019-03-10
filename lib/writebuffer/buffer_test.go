package writebuffer

import (
	"testing"

	"github.com/kshvakov/clickhouse/lib/leakypool"
	"github.com/stretchr/testify/assert"
)

func Test_WriteBuffer_SafeWithLeakyPool(t *testing.T) {
	leakypool.InitBytePool(1)
	wb := New(InitialSize)
	wb.Write(make([]byte, 1))
	leakypool.PutBytes(make([]byte, InitialSize))
	assert.NotPanics(t, func() { wb.Write(make([]byte, InitialSize+1)) })
}
