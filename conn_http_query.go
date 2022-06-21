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
	"bufio"
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"io"
	"strings"
)

func (h *httpConnect) query(ctx context.Context, query string, args ...interface{}) (*rows, error) {
	query, err := bind(h.location, query, args...)
	if err != nil {
		return nil, err
	}

	req, err := h.prepareRequest(ctx, strings.NewReader(query), nil)
	if err != nil {
		return nil, err
	}

	res, err := h.executeRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	decoder := binary.NewDecoder(bufio.NewReader(res))
	block, err := readData(decoder)
	if err != nil {
		return nil, err
	}

	var (
		errCh  = make(chan error)
		stream = make(chan *proto.Block, 2)
	)

	go func() {
		for {
			block, err := readData(decoder)
			if err != nil {
				if err != io.EOF {
					errCh <- err
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
		close(stream)
		close(errCh)
	}()

	return &rows{
		block:     block,
		stream:    stream,
		errors:    errCh,
		columns:   block.ColumnsNames(),
		structMap: &structMap{},
	}, nil
}
