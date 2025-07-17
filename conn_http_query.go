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
	"context"
	"errors"
	"fmt"
	"io"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// release is ignored, because http used by std with empty release function
func (h *httpConnect) query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error) {
	h.debugf("[http query] \"%s\"", query)
	options := queryOptions(ctx)
	query, err := bindQueryOrAppendParameters(true, &options, query, h.handshake.Timezone, args...)
	if err != nil {
		err = fmt.Errorf("bindQueryOrAppendParameters: %w", err)
		release(h, err)
		return nil, err
	}
	headers := make(map[string]string)
	switch h.compression {
	case CompressionZSTD, CompressionLZ4:
		options.settings["compress"] = "1"
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		// request encoding
		headers["Accept-Encoding"] = h.compression.String()
	}

	res, err := h.sendQuery(ctx, query, &options, headers)
	if err != nil {
		err = fmt.Errorf("sendQuery: %w", err)
		release(h, err)
		return nil, err
	}

	if res.ContentLength == 0 {
		discardAndClose(res.Body)
		block := proto.NewBlock()
		release(h, nil)
		return &rows{
			block:     block,
			columns:   block.ColumnsNames(),
			structMap: &structMap{},
		}, nil
	}

	rw := h.compressionPool.Get()
	// The HTTPReaderWriter.NewReader will create a reader that will decompress it if needed,
	// cause adding Accept-Encoding:gzip on your request means response won’t be automatically decompressed
	// per https://github.com/golang/go/blob/master/src/net/http/transport.go#L182-L190.
	// Note user will need to have set enable_http_compression for CH to respond with compressed data. we don't set this
	// automatically as they might not have permissions.
	reader, err := rw.NewReader(res)
	if err != nil {
		err = fmt.Errorf("NewReader: %w", err)
		discardAndClose(res.Body)
		h.compressionPool.Put(rw)
		release(h, err)
		return nil, err
	}
	chReader := chproto.NewReader(reader)
	block, err := h.readData(chReader, options.userLocation)
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("readData: %w", err)
		discardAndClose(res.Body)
		h.compressionPool.Put(rw)
		release(h, err)
		return nil, err
	}

	bufferSize := h.blockBufferSize
	if options.blockBufferSize > 0 {
		// allow block buffer size to be overridden per query
		bufferSize = options.blockBufferSize
	}
	var (
		errCh  = make(chan error)
		stream = make(chan *proto.Block, bufferSize)
	)
	go func() {
		for {
			block, err := h.readData(chReader, options.userLocation)
			if err != nil {
				// ch-go wraps EOF errors
				if !errors.Is(err, io.EOF) {
					errCh <- fmt.Errorf("readData stream: %w", err)
				}
				break
			}
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				break
			case stream <- block:
			}
		}
		discardAndClose(res.Body)
		h.compressionPool.Put(rw)
		close(stream)
		close(errCh)
		release(h, nil)
	}()

	if block == nil {
		block = proto.NewBlock()
	}

	return &rows{
		block:     block,
		stream:    stream,
		errors:    errCh,
		columns:   block.ColumnsNames(),
		structMap: &structMap{},
	}, nil
}

func (h *httpConnect) queryRow(ctx context.Context, release nativeTransportRelease, query string, args ...any) *row {
	rows, err := h.query(ctx, release, query, args...)
	if err != nil {
		return &row{
			err: err,
		}
	}

	return &row{
		rows: rows,
	}
}
