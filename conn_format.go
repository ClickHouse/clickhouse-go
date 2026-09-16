package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

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
	if c.server.Revision < proto.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_FORMATTED_RESULTS {
		release(c, nil)
		return nil, fmt.Errorf("%w: server revision is %d, need at least %d",
			ErrServerFormattedResultsUnsupported,
			c.server.Revision,
			proto.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_FORMATTED_RESULTS)
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
	if err := c.sendQueryPacket(body, &options, proto.ClientQueryWithServerFormattedResult); err != nil {
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

func (c *connect) insertFormat(_ context.Context, release nativeTransportRelease, _ string, _ string, _ io.Reader) error {
	// The current server extension formats query results only. Client-to-server
	// arbitrary-format streaming needs a separate packet and is not inferred
	// from the result protocol.
	release(c, nil)
	return ErrInsertFormatNativeUnsupported
}
