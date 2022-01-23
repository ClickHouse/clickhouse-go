package compress

import (
	"encoding/binary"
)

var endian = binary.LittleEndian

type Method byte

const (
	NONE Method = 0x02
	LZ4         = 0x82
	ZSTD        = 0x90
)

const (
	// ChecksumSize is 128bits for cityhash102 checksum
	checksumSize = 16
	// CompressHeader magic + compressed_size + uncompressed_size
	compressHeaderSize = 1 + 4 + 4
	headerSize         = checksumSize + compressHeaderSize
	maxBlockSize       = 1 << 20
)
