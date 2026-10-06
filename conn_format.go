package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

const maxFormattedPacketSize = 64 << 20

type nativeFormatStream struct {
	reader *io.PipeReader
	cancel context.CancelFunc
	done   <-chan struct{}
	once   sync.Once
}

func (s *nativeFormatStream) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *nativeFormatStream) Close() error {
	s.once.Do(func() {
		select {
		case <-s.done:
		default:
			s.cancel()
			_ = s.reader.Close()
			<-s.done
		}
	})
	return s.reader.Close()
}

func (c *connect) queryFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, args ...any) (io.ReadCloser, error) {
	if err := c.checkFormattedDataSupport(); err != nil {
		release(c, nil)
		return nil, err
	}

	options := queryOptions(ctx)
	queryParamsProtocolSupport := c.revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
	body, err := bindQueryOrAppendParameters(queryParamsProtocolSupport, &options, query, c.server.Timezone, args...)
	if err != nil {
		release(c, nil)
		return nil, fmt.Errorf("bindQueryOrAppendParameters: %w", err)
	}

	// The method argument is authoritative even when output_format is also set
	// through the connection or query context.
	options.settings["output_format"] = formatName
	if err := c.sendQueryWithDataEncoding(body, &options, proto.DataEncodingFormattedResult); err != nil {
		release(c, err)
		return nil, err
	}

	onProcess := options.onProcess()
	metadata, err := c.firstFormattedResult(ctx, onProcess)
	if err != nil {
		release(c, err)
		return nil, err
	}
	if metadata.format != formatName {
		err = fmt.Errorf("clickhouse: server selected output format %q, requested %q", metadata.format, formatName)
		release(c, err)
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		err := c.processFormattedResult(streamCtx, onProcess, writer)
		if err != nil {
			c.logger.Error("formatted query processing failed", slog.Any("error", err))
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
		release(c, err)
	}()

	return &nativeFormatStream{
		reader: reader,
		cancel: cancel,
		done:   done,
	}, nil
}

type formattedResultMetadata struct {
	format      string
	contentType string
}

func (c *connect) firstFormattedResult(ctx context.Context, on *onProcess) (formattedResultMetadata, error) {
	resultCh := make(chan formattedResultMetadata, 1)
	errCh := make(chan error, 1)
	go func() {
		metadata, err := c.firstFormattedResultImpl(ctx, on)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- metadata
	}()

	select {
	case <-ctx.Done():
		_ = c.cancel()
		return formattedResultMetadata{}, ctx.Err()
	case err := <-errCh:
		return formattedResultMetadata{}, err
	case metadata := <-resultCh:
		return metadata, nil
	}
}

func (c *connect) firstFormattedResultImpl(ctx context.Context, on *onProcess) (formattedResultMetadata, error) {
	c.readerMutex.Lock()
	defer c.readerMutex.Unlock()
	c.startReadWriteTimeout(ctx)
	defer c.clearReadWriteTimeout(ctx)

	for {
		packet, err := c.reader.ReadByte()
		if err != nil {
			return formattedResultMetadata{}, fmt.Errorf("query processing: failed to read result metadata: %w", err)
		}
		switch packet {
		case proto.ServerResultMetadata:
			format, err := readFormattedString(c.reader, maxFormattedPacketSize)
			if err != nil {
				return formattedResultMetadata{}, fmt.Errorf("read result format: %w", err)
			}
			contentType, err := readFormattedString(c.reader, maxFormattedPacketSize)
			if err != nil {
				return formattedResultMetadata{}, fmt.Errorf("read result content type: %w", err)
			}
			return formattedResultMetadata{format: string(format), contentType: string(contentType)}, nil
		case proto.ServerFormattedData:
			return formattedResultMetadata{}, errors.New("clickhouse: formatted data received before result metadata")
		case proto.ServerEndOfStream:
			return formattedResultMetadata{}, errors.New("clickhouse: formatted result ended before result metadata")
		default:
			if err := c.handle(ctx, packet, on); err != nil {
				return formattedResultMetadata{}, err
			}
		}
	}
}

func (c *connect) processFormattedResult(ctx context.Context, on *onProcess, writer *io.PipeWriter) error {
	errCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)
	go func() {
		if err := c.processFormattedResultImpl(ctx, on, writer); err != nil {
			errCh <- err
			return
		}
		doneCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		_ = c.cancel()
		return ctx.Err()
	case err := <-errCh:
		return err
	case <-doneCh:
		return nil
	}
}

func (c *connect) processFormattedResultImpl(ctx context.Context, on *onProcess, writer *io.PipeWriter) error {
	c.readerMutex.Lock()
	defer c.readerMutex.Unlock()
	c.startReadWriteTimeout(ctx)
	defer c.clearReadWriteTimeout(ctx)

	for {
		packet, err := c.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("query processing: failed to read formatted result packet: %w", err)
		}
		switch packet {
		case proto.ServerEndOfStream:
			return nil
		case proto.ServerResultMetadata:
			return errors.New("clickhouse: duplicate result metadata packet")
		case proto.ServerFormattedData:
			if c.compression != CompressionNone {
				c.reader.EnableCompression()
			}
			data, readErr := readFormattedString(c.reader, maxFormattedPacketSize)
			if c.compression != CompressionNone {
				c.reader.DisableCompression()
			}
			if readErr != nil {
				return fmt.Errorf("read formatted result data: %w", readErr)
			}
			if _, err := writer.Write(data); err != nil {
				return err
			}
		default:
			if err := c.handle(ctx, packet, on); err != nil {
				return err
			}
		}
	}
}

func readFormattedString(reader *chproto.Reader, maxSize int) ([]byte, error) {
	size, err := reader.StrLen()
	if err != nil {
		return nil, err
	}
	if size > maxSize {
		return nil, fmt.Errorf("packet payload is %d bytes, maximum is %d", size, maxSize)
	}
	data := make([]byte, size)
	if err := reader.ReadFull(data); err != nil {
		return nil, err
	}
	return data, nil
}

// checkFormattedDataSupport reports whether the server supports formatted data
// (server-side output and input formats) over the native protocol.
func (c *connect) checkFormattedDataSupport() error {
	if c.server.Revision < proto.DBMS_MIN_PROTOCOL_VERSION_WITH_FORMATTED_DATA {
		return fmt.Errorf("%w: server revision is %d, need at least %d",
			ErrServerFormattedDataUnsupported,
			c.server.Revision,
			proto.DBMS_MIN_PROTOCOL_VERSION_WITH_FORMATTED_DATA)
	}
	return nil
}

// formattedInputChunkSize is the size of the FormattedData packets of InsertFormat.
const formattedInputChunkSize = 1 << 20

// insertFormat streams data as is to the server, which parses it in formatName:
// the native protocol counterpart of the HTTP InsertFormat. The data is sent in
// FormattedData packets, followed by an empty one that ends it.
func (c *connect) insertFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, data io.Reader) error {
	if err := c.checkFormattedDataSupport(); err != nil {
		release(c, nil)
		return err
	}
	insertStmt, _, _, err := extractInsertQueryComponents(query)
	if err != nil {
		// Client-side parse failure: the connection is healthy and unused.
		release(c, nil)
		return err
	}

	options := queryOptions(ctx)
	// The format argument is authoritative: any FORMAT clause in the original
	// query was stripped by extractInsertQueryComponents.
	body := insertStmt + " FORMAT " + formatName
	if err := c.sendQueryWithDataEncoding(body, &options, proto.DataEncodingFormattedInput); err != nil {
		err = fmt.Errorf("insert %s: %w", formatName, err)
		release(c, err)
		return err
	}

	// The response is read while the data is sent: during the INSERT the server
	// sends progress, logs and profile events, which must not fill the
	// connection, and an exception stops sending early.
	var serverFailed atomic.Bool
	responseDone := make(chan error, 1)
	onProcess := options.onProcess()
	go func() {
		err := c.processImpl(ctx, onProcess)
		if err != nil {
			serverFailed.Store(true)
		}
		responseDone <- err
	}()

	if err := c.sendFormattedInput(ctx, data, &serverFailed); err != nil {
		// The data could not be read or sent. Canceling the query closes the
		// connection, so the server does not insert the data sent so far as if it
		// were complete, and the response reading ends.
		_ = c.cancel()
		<-responseDone
		err = fmt.Errorf("insert %s: %w", formatName, err)
		release(c, err)
		return err
	}

	select {
	case err = <-responseDone:
	case <-ctx.Done():
		_ = c.cancel()
		<-responseDone
		err = ctx.Err()
	}
	if err != nil {
		err = fmt.Errorf("insert %s: %w", formatName, err)
	}
	release(c, err)
	return err
}

// sendFormattedInput sends data in FormattedData packets, then the empty one
// that ends it. If the server has already failed, sending stops early; the end
// of the data is still sent, because the server reads the data up to it, to keep
// the connection usable. An error means that the data could not be read or sent.
func (c *connect) sendFormattedInput(ctx context.Context, data io.Reader, serverFailed *atomic.Bool) error {
	_, hasDeadline := ctx.Deadline()
	chunk := make([]byte, formattedInputChunkSize)
	for !serverFailed.Load() {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := io.ReadFull(data, chunk)
		if n > 0 {
			if err := c.sendFormattedData(chunk[:n]); err != nil {
				return err
			}
			// The response reader waits for at most ReadTimeout (without a context
			// deadline): the data that is being sent counts as activity.
			if !hasDeadline && c.readTimeout > 0 {
				_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read data: %w", readErr)
		}
	}
	return c.sendFormattedData(nil)
}

// sendFormattedData sends a FormattedData packet. Like the block of a Data
// packet, the body is compressed if compression is enabled. An empty fragment
// ends the data.
func (c *connect) sendFormattedData(data []byte) error {
	if c.isClosed() {
		return errors.New("attempted sending on closed connection")
	}
	c.buffer.PutByte(proto.ClientFormattedData)
	start := len(c.buffer.Buf)
	c.buffer.PutUVarInt(uint64(len(data)))
	c.buffer.PutRaw(data)
	if err := c.compressBuffer(start); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("send formatted data: %w", err)
	}
	return nil
}
