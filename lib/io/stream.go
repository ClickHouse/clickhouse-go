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

package io

import (
	"bufio"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/compress"
)

const (
	maxReaderSize = 1 << 20
	maxWriterSize = 1 << 20
)

func NewStream(rw io.ReadWriter) *Stream {
	stream := Stream{
		r: bufio.NewReaderSize(rw, maxReaderSize),
		w: bufio.NewWriterSize(rw, maxWriterSize),
	}
	stream.compress.r = compress.NewReader(stream.r)
	stream.compress.w = compress.NewWriter(stream.w)
	return &stream
}

type Stream struct {
	r        *bufio.Reader
	w        *bufio.Writer
	compress struct {
		enable bool
		r      *compress.Reader
		w      *compress.Writer
	}
}

func (s *Stream) Compress(v bool) {
	s.compress.enable = v
}

func (s *Stream) Read(p []byte) (int, error) {
	if s.compress.enable {
		return io.ReadFull(s.compress.r, p)
	}
	return io.ReadFull(s.r, p)
}

func (s *Stream) Write(p []byte) (int, error) {
	if s.compress.enable {
		return s.compress.w.Write(p)
	}
	return s.w.Write(p)
}

func (s *Stream) Flush() error {
	if err := s.compress.w.Flush(); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *Stream) Close() error {
	s.r, s.w = nil, nil
	s.compress.r.Close()
	s.compress.w.Close()
	return nil
}
