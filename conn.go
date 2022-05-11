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
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/io"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func dial(ctx context.Context, addr string, num int, opt *Options) (*connect, error) {
	var (
		err    error
		conn   net.Conn
		debugf = func(format string, v ...interface{}) {}
	)
	switch {
	case opt.DialContext != nil:
		conn, err = opt.DialContext(ctx, addr)
	default:
		switch {
		case opt.TLS != nil:
			conn, err = tls.DialWithDialer(&net.Dialer{Timeout: opt.DialTimeout}, "tcp", addr, opt.TLS)
		default:
			conn, err = net.DialTimeout("tcp", addr, opt.DialTimeout)
		}
	}
	if err != nil {
		return nil, err
	}
	if opt.Debug {
		if opt.Debugf != nil {
			debugf = opt.Debugf
		} else {
			debugf = log.New(os.Stdout, fmt.Sprintf("[clickhouse][conn=%d][%s]", num, conn.RemoteAddr()), 0).Printf
		}
	}
	var compression bool
	if opt.Compression != nil {
		compression = opt.Compression.Method == CompressionLZ4
	}
	var (
		stream  = io.NewStream(conn)
		connect = &connect{
			opt:         opt,
			conn:        conn,
			debugf:      debugf,
			stream:      stream,
			encoder:     binary.NewEncoder(stream),
			decoder:     binary.NewDecoder(stream),
			revision:    proto.ClientTCPProtocolVersion,
			structMap:   &structMap{},
			compression: compression,
			connectedAt: time.Now(),
		}
	)
	if err := connect.handshake(opt.Auth.Database, opt.Auth.Username, opt.Auth.Password); err != nil {
		return nil, err
	}
	return connect, nil
}

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
type connect struct {
	opt         *Options
	conn        net.Conn
	debugf      func(format string, v ...interface{})
	server      ServerVersion
	stream      *io.Stream
	closed      bool
	encoder     *binary.Encoder
	decoder     *binary.Decoder
	released    bool
	revision    uint64
	structMap   *structMap
	compression bool
	// lastUsedIn  time.Time
	connectedAt time.Time
}

func (c *connect) settings(querySettings Settings) []proto.Setting {
	settings := make([]proto.Setting, 0, len(c.opt.Settings)+len(querySettings))
	for k, v := range c.opt.Settings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: v,
		})
	}
	for k, v := range querySettings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: v,
		})
	}
	return settings
}

func (c *connect) isBad() bool {
	switch {
	case c.closed:
		return true
	}
	if err := c.connCheck(); err != nil {
		return true
	}
	return false
}

func (c *connect) close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.encoder = nil
	c.decoder = nil
	c.stream.Close()
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (c *connect) progress() (*Progress, error) {
	var progress proto.Progress
	if err := progress.Decode(c.decoder, c.revision); err != nil {
		return nil, err
	}
	c.debugf("[progress] %s", &progress)
	return &progress, nil
}

func (c *connect) exception() error {
	var e Exception
	if err := e.Decode(c.decoder); err != nil {
		return err
	}
	c.debugf("[exception] %s", e.Error())
	return &e
}

func (c *connect) sendData(block *proto.Block, name string) error {
	c.debugf("[send data] compression=%t", c.compression)
	if err := c.encoder.Byte(proto.ClientData); err != nil {
		return err
	}
	if err := c.encoder.String(name); err != nil {
		return err
	}
	if c.compression {
		c.stream.Compress(true)
		defer func() {
			c.stream.Compress(false)
			c.encoder.Flush()
		}()
	}
	return block.Encode(c.encoder, c.revision)
}

func (c *connect) readData(packet byte, compressible bool) (*proto.Block, error) {
	if _, err := c.decoder.String(); err != nil {
		return nil, err
	}
	if compressible && c.compression {
		c.stream.Compress(true)
		defer c.stream.Compress(false)
	}
	var block proto.Block
	if err := block.Decode(c.decoder, c.revision); err != nil {
		return nil, err
	}
	block.Packet = packet
	c.debugf("[read data] compression=%t. block: columns=%d, rows=%d", c.compression, len(block.Columns), block.Rows())
	return &block, nil
}
