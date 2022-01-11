package clickhouse

import (
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

func dial(addr string, num int, opt *Options) (*connect, error) {
	var (
		err    error
		conn   net.Conn
		debugf = func(format string, v ...interface{}) {}
	)
	if opt.Debug {
		debugf = log.New(os.Stdout, fmt.Sprintf("[clickhouse][conn=%d]", num), 0).Printf
	}
	switch {
	case opt.TLS != nil:
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: opt.DialTimeout}, "tcp", addr, opt.TLS)
	default:
		conn, err = net.DialTimeout("tcp", addr, opt.DialTimeout)
	}
	if err != nil {
		return nil, err
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
	err         error
	opt         *Options
	conn        net.Conn
	debugf      func(format string, v ...interface{})
	server      ServerVersion
	stream      *io.Stream
	closed      bool
	encoder     *binary.Encoder
	decoder     *binary.Decoder
	revision    uint64
	compression bool
	connectedAt time.Time
}

func (c *connect) settings(querySettings Settings) []proto.Setting {
	settings := make([]proto.Setting, 0, len(c.opt.Settings)+len(querySettings))
	for k, v := range c.opt.Settings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: fmt.Sprint(v),
		})
	}
	for k, v := range querySettings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: fmt.Sprint(v),
		})
	}
	return settings
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

func (c *connect) readData(compressible bool) (*proto.Block, error) {
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
	c.debugf("[read data] compression=%t. block: columns=%d, rows=%d", c.compression, len(block.Columns), block.Rows())
	return &block, nil
}
