package clickhouse

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func randomBytes(n int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	return data
}

// decompressBlocks checks that every compressed block in data holds at most
// maxCompressBlockSize bytes, and returns the decompressed data.
func decompressBlocks(t *testing.T, data []byte) []byte {
	t.Helper()
	var total int
	for b := data; len(b) > 0; {
		// Checksum (16 bytes), method (1 byte), compressed size including
		// the method and both sizes (4 bytes), uncompressed size (4 bytes).
		require.GreaterOrEqual(t, len(b), 25)
		blockSize := 16 + int(binary.LittleEndian.Uint32(b[17:]))
		require.LessOrEqual(t, blockSize, len(b))
		dataSize := int(binary.LittleEndian.Uint32(b[21:]))
		require.LessOrEqual(t, dataSize, maxCompressBlockSize)
		total += dataSize
		b = b[blockSize:]
	}
	out := make([]byte, total)
	_, err := io.ReadFull(compress.NewReader(bytes.NewReader(data)), out)
	require.NoError(t, err)
	return out
}

func TestCompressBufferBlockSize(t *testing.T) {
	data := randomBytes(2*maxCompressBlockSize + 100)

	for _, method := range []CompressionMethod{CompressionLZ4, CompressionLZ4HC, CompressionZSTD} {
		t.Run(method.String(), func(t *testing.T) {
			c := &connect{
				compression: method,
				compressor:  compress.NewWriter(compress.LevelZero, compress.Method(method)),
				buffer:      new(chproto.Buffer),
			}
			c.buffer.PutString("test")
			start := len(c.buffer.Buf)
			prefix := append([]byte(nil), c.buffer.Buf...)
			c.buffer.PutRaw(data)

			require.NoError(t, c.compressBuffer(start))
			require.Equal(t, prefix, c.buffer.Buf[:start])
			require.Equal(t, data, decompressBlocks(t, c.buffer.Buf[start:]))
		})
	}
}

func TestWriteDataBlockSize(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("s", column.Type("String")))
	value := string(randomBytes(maxCompressBlockSize))
	for i := 0; i < 3; i++ {
		require.NoError(t, block.Append(value))
	}
	var want chproto.Buffer
	require.NoError(t, block.Encode(&want, ClientTCPProtocolVersion))

	for _, method := range []CompressionMethod{CompressionLZ4, CompressionZSTD} {
		t.Run(method.String(), func(t *testing.T) {
			h := &httpConnect{
				compression:     method,
				blockCompressor: compress.NewWriter(compress.LevelZero, compress.Method(method)),
				buffer:          new(chproto.Buffer),
				encodeRevision:  ClientTCPProtocolVersion,
			}
			require.NoError(t, h.writeData(block))
			require.Equal(t, want.Buf, decompressBlocks(t, h.buffer.Buf))
		})
	}
}
