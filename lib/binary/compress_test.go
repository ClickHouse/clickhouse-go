package binary

import (
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	bklz4 "github.com/bkaradzic/go-lz4"
	cflz4 "github.com/cloudflare/golz4"
	ownlz4 "github.com/ClickHouse/clickhouse-go/lib/lz4"
	pilz4 "github.com/pierrec/lz4"
)

func Test_CompressCorrect(t *testing.T) {
	var cases = [][]byte{genBytes(5), genBytes(25), genBytes(255), genBytes(2555), genBytes(25555)}

	for _, c := range cases {
		// cfRes is the correct answer
		cfRes := GetCfEncode(c)

		ownRes := GetOwnEncode(c)
		bkRes := GetBkEncode(c)
		piRes := GetPiEncode(c)

		assert.Equal(t, cfRes, ownRes, "own not matched")
		assert.Equal(t, cfRes, bkRes, "bk not matched")
		assert.NotEqual(t, cfRes, piRes, "pi  matched")
	}
}

func Benchmark_CompressCf(b *testing.B) {
	var c = genBytes(1 << 10)
	for i := 0; i < b.N; i++ {
		GetCfEncode(c)
	}
}

// Poor performance mostly due to https://github.com/bkaradzic/go-lz4/issues/21
func Benchmark_CompressBk(b *testing.B) {
	var c = genBytes(1 << 10)
	for i := 0; i < b.N; i++ {
		GetBkEncode(c)
	}
}

func Benchmark_CompressOwn(b *testing.B) {
	var c = genBytes(1 << 10)
	for i := 0; i < b.N; i++ {
		GetOwnEncode(c)
	}
}

func genBytes(n int) []byte {
	var res = make([]byte, n)

	for i := 0; i < n; i++ {
		res[i] = byte(rand.Int() % 122)
	}
	return res
}

func GetCfEncode(in []byte) []byte {
	b := cflz4.CompressBound(in)
	out := make([]byte, b)
	compressedSize, err := cflz4.Compress(in, out)
	if err != nil {
		log.Fatal(err)
	}
	return out[:compressedSize]
}

func GetBkEncode(in []byte) []byte {
	b := bklz4.CompressBound(len(in))
	out := make([]byte, b)
	out2, err := bklz4.Encode(out, in)
	if err != nil {
		log.Fatal(err)
	}

	// here we should skip 4 bytes
	// go-lz4 saves a uint32 with the original uncompressed length at the beginning of the encoded buffer. They may get in the way of interoperability with other implementations.
	return out2[4:]
}

func GetPiEncode(in []byte) []byte {
	b := pilz4.CompressBlockBound(len(in))
	out := make([]byte, b)

	var hashTable = make([]int, 65536)
	compressedSize, err := pilz4.CompressBlock(in, out, hashTable)
	if err != nil {
		log.Fatal(err)
	}
	return out[:compressedSize]
}

func GetOwnEncode(in []byte) []byte {
	b := ownlz4.CompressBound(len(in))
	out := make([]byte, b)
	compressedSize, err := ownlz4.Encode(out, in)
	if err != nil {
		log.Fatal(err)
	}
	return out[:compressedSize]
}
