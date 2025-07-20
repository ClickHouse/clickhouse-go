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

package testing

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// TestServer represents a mock ClickHouse server for testing
type TestServer struct {
	listener net.Listener
	handlers PacketHandlers
	done     chan struct{}
}

// PacketHandlers contains handlers for different protocol packets
type PacketHandlers struct {
	// OnClientHandshake is called when a client handshake is received
	OnClientHandshake func(handshake proto.ClientHandshake) (proto.ServerHandshake, error)

	// OnQuery is called when a query packet is received
	OnQuery func(*proto.Query, []*proto.Block, chan<- *proto.Block) error

	// OnCancel is called when a cancel packet is received
	OnCancel func() error

	// OnPing is called when a ping packet is received
	OnPing func() error

	// OnUnknownPacket is called when an unknown packet type is received
	OnUnknownPacket func(packetType uint64, data []byte) error
}

// DefaultHandlers returns a set of default handlers that provide basic responses
func DefaultHandlers() PacketHandlers {
	return PacketHandlers{
		OnClientHandshake: func(handshake proto.ClientHandshake) (proto.ServerHandshake, error) {
			return proto.ServerHandshake{
				Name:        "ClickHouse",
				DisplayName: "ClickHouse Test Server",
				Revision:    proto.DBMS_MIN_REVISION_WITH_VERSION_PATCH,
				Version:     proto.Version{Major: 25, Minor: 6, Patch: 0},
				Timezone:    time.UTC,
			}, nil
		},
		OnQuery: func(*proto.Query, []*proto.Block, chan<- *proto.Block) error {
			// Default: do nothing, just acknowledge
			return nil
		},
		OnCancel: func() error {
			// Default: do nothing, just acknowledge
			return nil
		},
		OnPing: func() error {
			// Default: respond with pong
			return nil
		},
		OnUnknownPacket: func(packetType uint64, data []byte) error {
			return fmt.Errorf("unknown packet type: %d", packetType)
		},
	}
}

// NewTestServer creates a new test server with the given handlers
func NewTestServer(address string, handlers PacketHandlers) (*TestServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	return &TestServer{
		listener: listener,
		handlers: handlers,
		done:     make(chan struct{}),
	}, nil
}

// Start begins accepting connections and handling requests
func (ts *TestServer) Start() {
	go ts.acceptConnections()
}

// Stop stops the test server
func (ts *TestServer) Stop() error {
	close(ts.done)
	return ts.listener.Close()
}

// Address returns the address the server is listening on
func (ts *TestServer) Address() string {
	return ts.listener.Addr().String()
}

func (ts *TestServer) acceptConnections() {
	for {
		select {
		case <-ts.done:
			return
		default:
		}

		conn, err := ts.listener.Accept()
		if err != nil {
			select {
			case <-ts.done:
				return
			default:
				continue
			}
		}

		go ts.handleConnection(conn)
	}
}

// Helper method to get server revision
func (ts *TestServer) getRevision() uint64 {
	return proto.DBMS_MIN_REVISION_WITH_VERSION_PATCH
}

func (ts *TestServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := chproto.NewReader(conn)
	writer := chproto.NewWriter(conn, &chproto.Buffer{})

	for {
		select {
		case <-ts.done:
			return
		default:
		}

		if err := func() error {
			// Read packet type
			packetType, err := reader.UVarInt()
			if err != nil {
				return err
			}

			slog.Debug("Packet type", "type", packetType)

			if err := ts.handlePacket(reader, writer, packetType); err != nil {
				slog.Error("Handling packet", "error", err)
				// Send exception packet
				ts.sendException(writer, err)
			}

			return nil
		}(); err != nil {
			if err != io.EOF {
				return
			}
		}
	}
}

func (ts *TestServer) handlePacket(reader *chproto.Reader, writer *chproto.Writer, packetType uint64) error {
	switch packetType {
	case proto.ClientHello:
		return ts.handleClientHandshake(reader, writer)
	case proto.ClientQuery:
		return ts.handleQuery(reader, writer)
	case proto.ClientData:
		return errors.New("unexpected blocks")
	case proto.ClientCancel:
		return ts.handleCancel(reader, writer)
	case proto.ClientPing:
		return ts.handlePing(reader, writer)
	default:
		// Read remaining packet data
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		return ts.handlers.OnUnknownPacket(packetType, data)
	}
}

func (ts *TestServer) handleClientHandshake(reader *chproto.Reader, writer *chproto.Writer) (err error) {
	var handshake proto.ClientHandshake
	if err := handshake.Decode(reader); err != nil {
		return err
	}

	var auth struct {
		database string
		username string
		password string
	}

	{
		if auth.database, err = reader.Str(); err != nil {
			return err
		}
		if auth.username, err = reader.Str(); err != nil {
			return err
		}
		if auth.password, err = reader.Str(); err != nil {
			return err
		}
	}

	slog.Debug("Handling handshake", "handshake", handshake, "auth", auth)

	serverHandshake, err := ts.handlers.OnClientHandshake(handshake)
	if err != nil {
		return err
	}

	// Send server handshake response
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodeHello)
		serverHandshake.Encode(b)
	})

	_, err = writer.Flush()
	return err
}

func (ts *TestServer) handleQuery(reader *chproto.Reader, writer *chproto.Writer) error {
	query := &proto.Query{}
	if err := query.Decode(reader, proto.DBMS_MIN_REVISION_WITH_VERSION_PATCH); err != nil {
		return fmt.Errorf("handling query: %w", err)
	}

	inBlocks, err := ts.readBlocks(reader)
	if err != nil {
		return fmt.Errorf("handling query blocks: %w", err)
	}

	slog.Debug("Handling query", "query", query, "blocks", inBlocks)

	outBlocks := make(chan *proto.Block)
	go func() {
		defer close(outBlocks)
		err = ts.handlers.OnQuery(query, inBlocks, outBlocks)
	}()

	for block := range outBlocks {
		var berr error
		writer.ChainBuffer(func(b *chproto.Buffer) {
			b.PutUVarInt(ServerCodeData)
			b.PutString("") // is this where TableColumns would be sent?
			berr = block.Encode(b, proto.DBMS_MIN_REVISION_WITH_VERSION_PATCH)
		})
		if _, werr := writer.Flush(); werr != nil {
			return werr
		}
		if berr != nil {
			return berr
		}
	}

	// now we can check error from query as outBlocks is closed
	if err != nil {
		return err
	}

	// Send end of stream
	return ts.sendEndOfStream(writer)
}

func (ts *TestServer) readBlocks(reader *chproto.Reader) (blocks []*proto.Block, _ error) {
	for {
		// Read the next packet type
		packetType, err := reader.UVarInt()
		if err != nil {
			return nil, err
		}

		switch packetType {
		case proto.ClientData:
			// Use ch-go's built-in block decoding
			var block proto.Block
			if err := block.Decode(reader, ts.getRevision()); err != nil {
				return nil, fmt.Errorf("failed to decode data block: %v", err)
			}

			// If this is an empty block (0 rows), it signals end of data transmission
			if block.Rows() == 0 {
				return
			}

			blocks = append(blocks, &block)
		case proto.ClientCancel:
			// Query was cancelled
			return nil, ts.handlers.OnCancel()

		default:
			return nil, fmt.Errorf("unexpected packet type %d while reading query data blocks", packetType)
		}
	}
}

func (ts *TestServer) handleCancel(_ *chproto.Reader, _ *chproto.Writer) error {
	return ts.handlers.OnCancel()
}

func (ts *TestServer) handlePing(_ *chproto.Reader, writer *chproto.Writer) error {
	if err := ts.handlers.OnPing(); err != nil {
		return err
	}

	// Send pong response
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodePong)
	})

	_, err := writer.Flush()
	return err
}

func (ts *TestServer) sendEndOfStream(writer *chproto.Writer) error {
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodeEndOfStream)
	})

	_, err := writer.Flush()
	return err
}

func (ts *TestServer) sendException(writer *chproto.Writer, err error) error {
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodeException)
		b.PutUVarInt(1)           // Exception code
		b.PutString("TestServer") // Exception name
		b.PutString(err.Error())  // Exception message
		b.PutString("")           // Stack trace
		b.PutUVarInt(0)           // Nested exception flag
	})

	_, err = writer.Flush()
	return err
}

// Protocol packet type constants (these would typically be defined elsewhere)
const (
	ServerCodeHello                = 0
	ServerCodeData                 = 1
	ServerCodeException            = 2
	ServerCodeProgress             = 3
	ServerCodePong                 = 4
	ServerCodeEndOfStream          = 5
	ServerCodeProfileInfo          = 6
	ServerCodeTotals               = 7
	ServerCodeExtremes             = 8
	ServerCodeTablesStatusResponse = 9
	ServerCodeLog                  = 10
	ServerCodeTableColumns         = 11
)

// Test helper functions
func (ts *TestServer) SendData(writer *chproto.Writer, data []byte) error {
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodeData)
		b.PutRaw(data)
	})

	_, err := writer.Flush()
	return err
}

func (ts *TestServer) SendProgress(writer *chproto.Writer, readRows, readBytes, totalRows uint64) error {
	writer.ChainBuffer(func(b *chproto.Buffer) {
		b.PutUVarInt(ServerCodeProgress)
		b.PutUVarInt(readRows)
		b.PutUVarInt(readBytes)
		b.PutUVarInt(totalRows)
	})

	_, err := writer.Flush()
	return err
}
