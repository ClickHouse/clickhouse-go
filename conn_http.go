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

package clickhouse

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/andybalholm/brotli"
	"github.com/pkg/errors"
)

const (
	quotaKeyParamName = "quota_key"
	queryIDParamName  = "query_id"
)

type Pool[T any] struct {
	pool *sync.Pool
}

func NewPool[T any](fn func() T) Pool[T] {
	return Pool[T]{
		pool: &sync.Pool{New: func() interface{} { return fn() }},
	}
}

func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}

type HTTPReaderWriter struct {
	reader io.Reader
	writer io.WriteCloser
	err    error
	method CompressionMethod
}

func (rw HTTPReaderWriter) read(res *http.Response) ([]byte, error) {
	enc := res.Header.Get("Content-Encoding")
	if !res.Uncompressed && rw.method.String() == enc {
		switch rw.method {
		case CompressionGZIP:
			reader := rw.reader.(*gzip.Reader)
			defer reader.Close()
			if err := reader.Reset(res.Body); err != nil {
				return nil, err
			}
			body, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			return body, nil
		case CompressionDeflate:
			reader := rw.reader.(io.ReadCloser)
			defer reader.Close()
			if err := rw.reader.(flate.Resetter).Reset(res.Body, nil); err != nil {
				return nil, err
			}
			body, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			return body, nil
		case CompressionBrotli:
			reader := rw.reader.(*brotli.Reader)
			if err := reader.Reset(res.Body); err != nil {
				return nil, err
			}
			body, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			return body, nil
		}
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (rw *HTTPReaderWriter) reset(pw *io.PipeWriter) io.WriteCloser {
	switch rw.method {
	case CompressionGZIP:
		rw.writer.(*gzip.Writer).Reset(pw)
		return rw.writer
	case CompressionDeflate:
		rw.writer.(*zlib.Writer).Reset(pw)
		return rw.writer
	case CompressionBrotli:
		rw.writer.(*brotli.Writer).Reset(pw)
		return rw.writer
	default:
		return pw
	}
}

func dialHttp(ctx context.Context, addr string, num int, opt *Options) (*httpConnect, error) {
	if opt.scheme == "" {
		switch opt.Protocol {
		case HTTP:
			opt.scheme = opt.Protocol.String()
			if opt.TLS != nil {
				opt.scheme = fmt.Sprintf("%ss", opt.scheme)
			}
		default:
			return nil, errors.New("invalid interface type for http")
		}
	}
	u := &url.URL{
		Scheme: opt.scheme,
		Host:   addr,
	}

	if len(opt.Auth.Username) > 0 {
		if len(opt.Auth.Password) > 0 {
			u.User = url.UserPassword(opt.Auth.Username, opt.Auth.Password)
		} else {
			u.User = url.User(opt.Auth.Username)
		}
	}

	query := u.Query()
	if len(opt.Auth.Database) > 0 {
		query.Set("database", opt.Auth.Database)
	}

	if opt.Compression == nil {
		opt.Compression = &Compression{
			Method: CompressionNone,
		}
	}

	compressionPool, err := createCompressionPool(opt.Compression)
	if err != nil {
		return nil, err
	}

	for k, v := range opt.Settings {
		query.Set(k, fmt.Sprint(v))
	}

	query.Set("default_format", "Native")
	u.RawQuery = query.Encode()

	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   opt.DialTimeout,
			KeepAlive: opt.ConnMaxLifetime,
		}).DialContext,
		MaxIdleConns:          1,
		IdleConnTimeout:       opt.ConnMaxLifetime,
		ResponseHeaderTimeout: opt.ReadTimeout,
		TLSClientConfig:       opt.TLS,
	}

	conn := &httpConnect{
		client: &http.Client{
			Transport: t,
		},
		url:             u,
		buffer:          new(chproto.Buffer),
		compression:     opt.Compression.Method,
		blockCompressor: compress.NewWriter(),
		compressionPool: compressionPool,
	}

	rows, err := conn.query(ctx, func(*connect, error) {}, "SELECT timeZone()")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var serverLocation string
		rows.Scan(&serverLocation)
		location, err := time.LoadLocation(serverLocation)
		if err != nil {
			return nil, err
		}
		conn.location = location
	}

	return conn, nil
}

type httpConnect struct {
	url             *url.URL
	client          *http.Client
	location        *time.Location
	buffer          *chproto.Buffer
	compression     CompressionMethod
	blockCompressor *compress.Writer
	compressionPool Pool[HTTPReaderWriter]
}

func (h *httpConnect) isBad() bool {
	if h.client == nil {
		return true
	}
	return false
}

func createCompressionPool(compression *Compression) (Pool[HTTPReaderWriter], error) {
	pool := NewPool(func() HTTPReaderWriter {
		switch compression.Method {
		case CompressionGZIP:
			// trick so we can init the reader to something to Reset when we reuse
			writer, err := gzip.NewWriterLevel(io.Discard, compression.Level)
			if err != nil {
				return HTTPReaderWriter{err: err}
			}
			b := new(bytes.Buffer)
			writer.Reset(b)
			writer.Flush()
			writer.Close()
			reader, err := gzip.NewReader(bytes.NewReader(b.Bytes()))
			return HTTPReaderWriter{writer: writer, reader: reader, err: err, method: compression.Method}
		case CompressionDeflate:
			writer, err := zlib.NewWriterLevel(io.Discard, compression.Level)
			if err != nil {
				return HTTPReaderWriter{err: err}
			}
			b := new(bytes.Buffer)
			writer.Reset(b)
			writer.Flush()
			writer.Close()
			reader, err := zlib.NewReader(bytes.NewReader(b.Bytes()))
			if err != nil {
				return HTTPReaderWriter{err: err}
			}
			return HTTPReaderWriter{writer: writer, reader: reader, method: compression.Method}
		case CompressionBrotli:
			writer := brotli.NewWriterLevel(io.Discard, compression.Level)
			b := new(bytes.Buffer)
			writer.Reset(b)
			writer.Flush()
			writer.Close()
			reader := brotli.NewReader(bytes.NewReader(b.Bytes()))
			return HTTPReaderWriter{writer: writer, reader: reader, method: compression.Method}
		default:
			return HTTPReaderWriter{method: CompressionNone}
		}
	})
	err := pool.Get().err
	if err != nil {
		return pool, err
	}
	return pool, nil
}

func (h *httpConnect) writeData(block *proto.Block) error {
	// Saving offset of compressible data
	start := len(h.buffer.Buf)
	if err := block.Encode(h.buffer, 0); err != nil {
		return err
	}
	if h.compression == CompressionLZ4 || h.compression == CompressionZSTD {
		// Performing compression. Supported and requires
		data := h.buffer.Buf[start:]
		if err := h.blockCompressor.Compress(compress.Method(h.compression), data); err != nil {
			return errors.Wrap(err, "compress")
		}
		h.buffer.Buf = append(h.buffer.Buf[:start], h.blockCompressor.Data...)
	}
	return nil
}

func (h *httpConnect) readData(reader *chproto.Reader) (*proto.Block, error) {
	var block proto.Block
	if h.compression == CompressionLZ4 || h.compression == CompressionZSTD {
		reader.EnableCompression()
		defer reader.DisableCompression()
	}
	if err := block.Decode(reader, 0); err != nil {
		return nil, err
	}
	return &block, nil
}

func (h *httpConnect) sendQuery(ctx context.Context, r io.Reader, options *QueryOptions, headers map[string]string) (*http.Response, error) {
	req, err := h.prepareRequest(ctx, r, options, headers)
	if err != nil {
		return nil, err
	}

	res, err := h.executeRequest(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func readResponse(response *http.Response) ([]byte, error) {
	var result []byte
	if response.ContentLength > 0 {
		result = make([]byte, 0, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err := buf.ReadFrom(response.Body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *httpConnect) prepareRequest(ctx context.Context, reader io.Reader, options *QueryOptions, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url.String(), reader)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	var query url.Values
	if options != nil {
		query = req.URL.Query()
		if options.queryID != "" {
			query.Set(queryIDParamName, options.queryID)
		}
		if options.quotaKey != "" {
			query.Set(quotaKeyParamName, options.quotaKey)
		}
		for key, value := range options.settings {
			// check that query doesn't change format
			if key == "default_format" {
				continue
			}
			query.Set(key, fmt.Sprint(value))
		}
		req.URL.RawQuery = query.Encode()
	}

	return req, nil
}

func (h *httpConnect) executeRequest(req *http.Request) (*http.Response, error) {
	if h.client == nil {
		return nil, driver.ErrBadConn
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg, err := readResponse(resp)

		if err != nil {
			return nil, errors.Wrap(err, "clickhouse [execute]:: failed to read the response")
		}

		return nil, fmt.Errorf("clickhouse [execute]:: %d code: %s", resp.StatusCode, string(msg))
	}
	return resp, nil
}

func (h *httpConnect) ping(ctx context.Context) error {
	rows, err := h.query(ctx, nil, "SELECT 1")
	if err != nil {
		return err
	}
	column := rows.Columns()
	// check that we got column 1
	if len(column) == 1 && column[0] == "1" {
		return nil
	}
	return errors.New("clickhouse [ping]:: cannot ping clickhouse")
}

func (h *httpConnect) close() error {
	if h.client == nil {
		return nil
	}
	h.client.CloseIdleConnections()
	h.client = nil
	return nil
}
