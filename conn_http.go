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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func dialHttp(ctx context.Context, addr string, num int, opt *Options) (*httpConnect, error) {
	url := &url.URL{
		Scheme: "http",
		Host:   addr,
	}

	if opt.TLS != nil {
		url.Scheme = "https"
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
		url:      url,
		location: time.UTC, // TODO: make configurable
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

func (h *httpConnect) prepareRequest(ctx context.Context, query string, args ...interface{}) (*http.Request, error) {
	query, err := bind(h.location, query, args)
	if err != nil {
		return nil, err
	}

	reader := strings.NewReader(query)

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

func (h *httpConnect) ping(ctx context.Context) error {
	req, err := h.prepareRequest(ctx, "SELECT 1")
	if err != nil {
		return err
	}

	res, err := h.executeRequest(ctx, req)
	if err != nil {
		return err
	}
	s, err := io.ReadAll(res)
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(s)) != "1" {
		return fmt.Errorf("clickhouse [ping]:: expected result (1), got '%s' instead", string(s))
	}

	return nil
}

func (h *httpConnect) close() error {
	if h.transport == nil {
		return nil
	}
	h.transport.CloseIdleConnections()
	h.transport = nil
	return nil
}
