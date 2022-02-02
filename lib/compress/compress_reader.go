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
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:      r,
		pos:    maxBlockSize,
		data:   make([]byte, maxBlockSize),
		zdata:  make([]byte, lz4.CompressBlockBound(maxBlockSize)+headerSize),
		header: make([]byte, headerSize),
	}
}

type Reader struct {
	r      io.Reader
	pos    int
	data   []byte
	zdata  []byte
	header []byte
}

func (r *Reader) Read(p []byte) (int, error) {
	bytesRead, n := 0, len(p)
	if r.pos < len(r.data) {
		copyedSize := copy(p, r.data[r.pos:])
		{
			bytesRead += copyedSize
			r.pos += copyedSize
		}
	}
	for bytesRead < n {
		if err := r.readBlock(); err != nil {
			return bytesRead, err
		}
		copyedSize := copy(p[bytesRead:], r.data)
		{
			bytesRead += copyedSize
			r.pos = copyedSize
		}
	}
	return n, nil
}

func (r *Reader) readBlock() (err error) {
	r.pos = 0
	var n int
	if n, err = io.ReadFull(r.r, r.header); err != nil {
		return
	}
	if n != len(r.header) {
		return fmt.Errorf("LZ4 decompression header EOF")
	}
	var (
		compressedSize   = int(endian.Uint32(r.header[17:])) - 9
		decompressedSize = int(endian.Uint32(r.header[21:]))
	)
	if compressedSize > cap(r.zdata) {
		r.zdata = make([]byte, compressedSize)
	}
	if decompressedSize > cap(r.data) {
		r.data = make([]byte, decompressedSize)
	}

	r.data, r.zdata = r.data[:decompressedSize], r.zdata[:compressedSize]

	switch r.header[16] {
	case LZ4:
	default:
		return fmt.Errorf("unknown compression method: 0x%02x ", r.header[16])
	}
	// @TODO checksum
	if n, err = io.ReadFull(r.r, r.zdata); err != nil {
		return
	}
	if n != len(r.zdata) {
		return fmt.Errorf("decompress read size not match")
	}
	if _, err = lz4.UncompressBlock(r.zdata, r.data); err != nil {
		return
	}
	return nil
}

func (r *Reader) Close() error {
	r.data = nil
	r.zdata = nil
	return nil
}
