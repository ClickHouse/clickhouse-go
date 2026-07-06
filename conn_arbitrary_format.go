package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	chformat "github.com/ClickHouse/clickhouse-go/v2/lib/format" // aliased: format collides with bind.go's format()
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// maxInsertBlockRows caps the rows per block decoded from an arbitrary-format
// insert payload, matching ClickHouse's DEFAULT_BLOCK_SIZE.
const maxInsertBlockRows = 65_409

func (c *connect) lookupFormatCodec(name string) (chformat.Codec, error) {
	codec, ok := c.opt.formatCodec(name)
	if !ok {
		return nil, fmt.Errorf("clickhouse: format %q has no client-side codec for the native protocol; "+
			"register one via Options.FormatCodecs or use Protocol: clickhouse.HTTP where the server converts all formats", name)
	}
	return codec, nil
}

func (c *connect) queryArbitraryFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, args ...any) (io.ReadCloser, error) {
	codec, err := c.lookupFormatCodec(formatName)
	if err != nil {
		// The connection is healthy and unused - release it back to the pool.
		release(c, nil)
		return nil, err
	}

	options := queryOptions(ctx)
	onProcess := options.onProcess()
	queryParamsProtocolSupport := c.revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
	body, err := bindQueryOrAppendParameters(queryParamsProtocolSupport, &options, query, c.server.Timezone, args...)
	if err != nil {
		c.logger.Error("failed to bind query parameters", slog.Any("error", err))
		release(c, err)
		return nil, err
	}

	// The query is sent unmodified: over the native protocol the server streams
	// Native blocks regardless of any FORMAT clause. The format argument only
	// selects the client-side codec.
	if err = c.sendQuery(body, &options); err != nil {
		release(c, err)
		return nil, err
	}

	first, err := c.firstBlock(ctx, onProcess)
	if errors.Is(err, io.EOF) {
		// No result stream; emit only the codec trailer.
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf)
		err = enc.Close()
		release(c, err)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(&buf), nil
	}
	if err != nil {
		c.logger.Error("failed to get first block", slog.Any("error", err))
		release(c, err)
		return nil, err
	}

	pr, pw := io.Pipe()
	enc := codec.NewEncoder(pw)

	// This goroutine owns the connection. It exits when the server sends
	// EndOfStream, on a protocol error, or when ctx is cancelled (process
	// observes ctx.Done and kills the socket via c.cancel). If the caller
	// closes the returned stream early, pipe writes fail with io.ErrClosedPipe:
	// encoding stops but the result keeps draining so the connection is
	// released healthy - the same semantics as rows.Close.
	go func() {
		// The first block usually carries only the schema; encoding it lets
		// header-emitting formats (e.g. *WithNames) write their header.
		encErr := enc.WriteBlock(first)
		onProcess.data = func(b *proto.Block) {
			if encErr != nil || b.Packet == proto.ServerTotals || b.Packet == proto.ServerExtremes {
				return
			}
			encErr = enc.WriteBlock(b)
		}
		procErr := c.process(ctx, onProcess)
		if procErr == nil && encErr == nil {
			encErr = enc.Close()
		}
		if errors.Is(encErr, io.ErrClosedPipe) {
			encErr = nil // the reader walked away; not an error
		}
		pw.CloseWithError(errors.Join(procErr, encErr))
		release(c, procErr)
	}()

	return pr, nil
}

func (c *connect) insertArbitraryFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, data io.Reader) error {
	codec, err := c.lookupFormatCodec(formatName)
	if err != nil {
		// The connection is healthy and unused - release it back to the pool.
		release(c, nil)
		return err
	}

	// The insert handshake speaks Native regardless of the payload format: the
	// client decodes the payload and ships Native blocks, mirroring clickhouse-client.
	normQuery, _, queryColumns, err := extractNormalizedInsertQueryAndColumns(query)
	if err != nil {
		release(c, err)
		return err
	}

	options := queryOptions(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}

	onProcess := options.onProcess()
	if err = c.sendQuery(normQuery, &options); err != nil {
		release(c, err)
		return err
	}

	// The server responds with a sample block carrying the table schema.
	block, err := c.firstBlock(ctx, onProcess)
	if err != nil {
		release(c, err)
		return err
	}
	if err = block.SortColumns(queryColumns); err != nil {
		release(c, err)
		return err
	}

	dec := codec.NewDecoder(data)
	for {
		block.Reset()
		n, decErr := dec.ReadBlock(block, maxInsertBlockRows)
		if n > 0 {
			if err = c.sendData(block, ""); err != nil {
				release(c, err)
				return err
			}
		}
		if decErr == nil {
			continue
		}
		if errors.Is(decErr, io.EOF) {
			break
		}
		// Abort the server-side INSERT; cancel closes the connection.
		if cErr := c.cancel(); cErr != nil {
			c.logger.Error("cancel failed after decode error", slog.Any("error", cErr))
		}
		decErr = fmt.Errorf("clickhouse: decoding %s insert payload: %w", formatName, decErr)
		release(c, decErr)
		return decErr
	}

	// An empty block marks the end of the insert data.
	if err = c.sendData(proto.NewBlock(), ""); err != nil {
		release(c, err)
		return err
	}
	if err = c.process(ctx, onProcess); err != nil {
		release(c, err)
		return err
	}
	release(c, nil)
	return nil
}
