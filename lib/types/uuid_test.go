package types

import (
	"github.com/stretchr/testify/assert"

	"encoding/hex"
	"testing"
)

func Test_UUID2Bytes(t *testing.T) {
	bytes2uuid := func(src []byte) string {
		var uuid [36]byte
		hex.Encode(uuid[:], src[:4])
		uuid[8] = '-'
		hex.Encode(uuid[9:13], src[4:6])
		uuid[13] = '-'
		hex.Encode(uuid[14:18], src[6:8])
		uuid[18] = '-'
		hex.Encode(uuid[19:23], src[8:10])
		uuid[23] = '-'
		hex.Encode(uuid[24:], src[10:])
		return string(uuid[:])
	}
	origin := "00000000-0000-0000-0000-000000000000"
	if uuid, err := uuid2bytes(origin); assert.NoError(t, err) {
		assert.Equal(t, origin, bytes2uuid(uuid))
	}
}

func Benchmark_UUID2Bytes(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := uuid2bytes("00000000-0000-0000-0000-000000000000"); err != nil {
			b.Fatal(err)
		}
	}
}
