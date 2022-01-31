// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
