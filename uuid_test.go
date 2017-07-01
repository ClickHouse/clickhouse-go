package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_UUID(t *testing.T) {
	origin := "123e4567-e89b-12d3-a456-426655440000"
	if uuid, err := uuid2bytes(origin); assert.NoError(t, err) {
		assert.Equal(t, origin, bytes2uuid(uuid))
	}
}

func Benchmark_UUID2Bytes(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := uuid2bytes("123e4567-e89b-12d3-a456-426655440000"); err != nil {
			b.Fatal(err)
		}
	}
}
