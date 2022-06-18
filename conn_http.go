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
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func dialHttp(ctx context.Context, addr string, num int, opt *Options) (*httpConnect, error) {
	u := &url.URL{
		Scheme: "http",
		Host:   addr,
	}

	query := u.Query()
	query.Set("default_format", "Native")
	u.RawQuery = query.Encode()

	if opt.TLS != nil {
		u.Scheme = "https"
	}

	connect := &httpConnect{
		opt: opt,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   opt.DialTimeout,
				KeepAlive: opt.ConnMaxLifetime,
			}).DialContext,
			MaxIdleConns:          1,
			IdleConnTimeout:       opt.ConnMaxLifetime,
			ResponseHeaderTimeout: opt.ReadTimeout,
			TLSClientConfig:       opt.TLS,
		},
		url:      u,
		location: time.UTC, // TODO: make configurables
	}

	if err := connect.ping(ctx); err != nil {
		return nil, err
	}

	return connect, nil
}

type httpConnect struct {
	opt       *Options
	url       *url.URL
	transport *http.Transport
	location  *time.Location
}

func (h *httpConnect) readData(decoder *binary.Decoder) (*proto.Block, error) {
	var block proto.Block
	if err := block.Decode(decoder, 0); err != nil {
		return nil, err
	}
	return &block, nil
}

func (h *httpConnect) prepareRequest(ctx context.Context, reader io.Reader) (*http.Request, error) {

	req, err := http.NewRequest(http.MethodPost, h.url.String(), reader)

	return req, err
}

func (h *httpConnect) executeRequest(ctx context.Context, req *http.Request) (io.ReadCloser, error) {

	if h.transport == nil {
		return nil, driver.ErrBadConn
	}

	resp, err := h.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse [execute]:: got no 200 code('%d')", resp.StatusCode)
	}

	return resp.Body, nil
}

func (h *httpConnect) exec(ctx context.Context, query string, args ...interface{}) error {
	query, err := bind(h.location, query, args...)
	if err != nil {
		return err
	}

	req, err := h.prepareRequest(ctx, strings.NewReader(query))
	if err != nil {
		return err
	}

	res, err := h.executeRequest(ctx, req)
	if res != nil {
		defer res.Close()
		// we don't care about result, so just discard it to reuse connection
		_, _ = io.Copy(ioutil.Discard, res)
	}

	return err
}

func (h *httpConnect) query(ctx context.Context, query string, args ...interface{}) (*rows, error) {

	query, err := bind(h.location, query, args...)
	if err != nil {
		return nil, err
	}

	req, err := h.prepareRequest(ctx, strings.NewReader(query))
	if err != nil {
		return nil, err
	}

	res, err := h.executeRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	decoder := binary.NewDecoder(res)
	block, err := h.readData(decoder)
	if err != nil {
		return nil, err
	}

	var (
		errCh  = make(chan error)
		stream = make(chan *proto.Block, 2)
	)

	go func() {
		for {
			block, err := h.readData(decoder)
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				close(stream)
				close(errCh)
				return
			}
			stream <- block
		}
	}()

	return &rows{
		block:     block,
		stream:    stream,
		errors:    errCh,
		columns:   block.ColumnsNames(),
		structMap: &structMap{},
	}, nil
}

func (h *httpConnect) ping(ctx context.Context) error {
	rows, err := h.query(ctx, "SELECT 1")
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
	if h.transport == nil {
		return nil
	}
	h.transport.CloseIdleConnections()
	h.transport = nil
	return nil
}
