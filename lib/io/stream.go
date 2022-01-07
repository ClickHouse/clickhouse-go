package io

import (
	"bufio"
	"io"

	"github.com/ClickHouse/clickhouse-go/lib/compress"
)

const (
	maxReadSizd = 256 << 10
	maxDataSize = 64 << 20
)

func NewStream(rw io.ReadWriter) *Stream {
	stream := Stream{
		r: bufio.NewReaderSize(rw, maxReadSizd),
		w: bufio.NewWriterSize(rw, maxDataSize),
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
