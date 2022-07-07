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
	"context"
	"database/sql/driver"
	"fmt"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
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
		Scheme: opt.Scheme,
		Host:   addr,
	}

	query := u.Query()
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
		opt: opt,
		client: &http.Client{
			Transport: t,
		},
		url:    u,
		buffer: new(chproto.Buffer),
		reader: &chproto.Reader{},
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
	opt      *Options
	url      *url.URL
	client   *http.Client
	location *time.Location
	buffer   *chproto.Buffer
	reader   *chproto.Reader
}

func (h *httpConnect) isBad() bool {
	if h.client == nil {
		return true
	}
	return false
}

func (h *httpConnect) writeData(block *proto.Block) error {
	return block.Encode(h.buffer, 0)
}

func (h *httpConnect) readData() (*proto.Block, error) {
	var block proto.Block
	if err := block.Decode(h.reader, 0); err != nil {
		return nil, err
	}
	return &block, nil
}

func (h *httpConnect) asyncInsert(ctx context.Context, query string, wait bool) error {
	return errors.New("HTTP: not supported")
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

func (h *httpConnect) prepareRequest(ctx context.Context, reader io.Reader, extra map[string]string) (*http.Request, error) {

	u := *h.url

	if len(extra) != 0 {
		query := u.Query()
		for key, value := range extra {
			query.Set(key, value)
		}
		u.RawQuery = query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), reader)

	return req, err
}

func (h *httpConnect) executeRequest(req *http.Request) (io.ReadCloser, error) {

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

	return resp.Body, nil
}

func (h *httpConnect) exec(ctx context.Context, query string, args ...interface{}) error {
	query, err := bind(h.location, query, args...)
	if err != nil {
		return err
	}

	req, err := h.prepareRequest(ctx, strings.NewReader(query), nil)
	if err != nil {
		return err
	}

	res, err := h.executeRequest(req)
	if res != nil {
		defer res.Close()
		// we don't care about result, so just discard it to reuse connection
		_, _ = io.Copy(ioutil.Discard, res)
	}

	return err
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
