package column

import (
	"github.com/stretchr/testify/assert"

	"encoding/hex"
	"testing"
)

func bytes2uuid(src []byte) string {
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

func Test_UUID2Bytes(t *testing.T) {
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

func Test_EmptyStringToNullUUID(t *testing.T) {
	origin := ""
	if uuid, err := uuid2bytes(origin); assert.NoError(t, err) {
		assert.Equal(t, "00000000-0000-0000-0000-000000000000", bytes2uuid(uuid))
	}
}

func Test_ErrInvalidUUIDFormat(t *testing.T) {
	cases := []struct {
		origin        string
		exceptedError error
	}{
		{
			"",
			nil,
		},
		{
			"a",
			ErrInvalidUUIDFormat,
		},
		{
			"00000000-0000-0000-00000000000000000",
			ErrInvalidUUIDFormat,
		},
		{
			"00000000-0000-0000-0000-0000000000000",
			ErrInvalidUUIDFormat,
		},
	}

	for _, Case := range cases {
		_, err := uuid2bytes(Case.origin)
		if Case.exceptedError != nil {
			assert.Error(t, err)
			assert.EqualError(t, Case.exceptedError, err.Error())
		} else {
			assert.NoError(t, err)
		}

	}
}
