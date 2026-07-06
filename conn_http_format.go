package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// exceptionScanLimit bounds how much of the remaining stream is buffered to
// parse a mid-stream exception once its marker is seen.
const exceptionScanLimit = 32 << 10

var exceptionMarker = []byte("__exception__")

// exceptionScanReader passes bytes through while scanning for the mid-stream
// exception marker ClickHouse appends to an HTTP response when a query fails
// after streaming has begun. On a match, the data before the marker is served
// and the next Read returns the parsed exception. A payload legitimately
// containing the marker is a false positive - a limitation of marker-based
// detection; callers needing all-or-nothing semantics should set
// wait_end_of_query=1.
type exceptionScanReader struct {
	src     io.Reader
	buf     []byte
	pending []byte
	err     error // terminal error: parsed exception or upstream read error
	srcDone bool
}

func newExceptionScanReader(src io.Reader) *exceptionScanReader {
	return &exceptionScanReader{src: src, buf: make([]byte, 32<<10)}
}

func (r *exceptionScanReader) Read(p []byte) (int, error) {
	for {
		if safe := r.safeLen(); safe > 0 {
			n := copy(p, r.pending[:safe])
			r.pending = r.pending[n:]
			return n, nil
		}
		if r.err != nil {
			return 0, r.err
		}
		if r.srcDone {
			return 0, io.EOF
		}

		n, err := r.src.Read(r.buf)
		if n > 0 {
			r.pending = append(r.pending, r.buf[:n]...)
			if i := bytes.Index(r.pending, exceptionMarker); i >= 0 {
				r.captureException(i)
			}
		}
		if err != nil {
			r.srcDone = true
			if !errors.Is(err, io.EOF) && r.err == nil {
				r.err = err
			}
		}
	}
}

// safeLen returns how many pending bytes can be served without risking that
// their tail is the beginning of an exception marker split across reads.
func (r *exceptionScanReader) safeLen() int {
	if r.srcDone || r.err != nil {
		return len(r.pending)
	}
	holdback := len(exceptionMarker) - 1
	if holdback > len(r.pending) {
		holdback = len(r.pending)
	}
	for k := holdback; k > 0; k-- {
		if bytes.Equal(r.pending[len(r.pending)-k:], exceptionMarker[:k]) {
			return len(r.pending) - k
		}
	}
	return len(r.pending)
}

// captureException truncates pending to the data before the marker and turns
// the marker plus the remainder of the stream into the terminal error.
func (r *exceptionScanReader) captureException(i int) {
	exc := append([]byte{}, r.pending[i:]...)
	r.pending = r.pending[:i]
	rest, _ := io.ReadAll(io.LimitReader(r.src, exceptionScanLimit))
	exc = append(exc, rest...)
	r.err = parseExceptionFromBytes(exc)
	r.srcDone = true
}

// httpFormatStream is the io.ReadCloser returned by queryFormat. It
// holds the connection until closed; Close drains the body so the HTTP
// connection stays reusable, then releases exactly once.
type httpFormatStream struct {
	reader  io.Reader
	body    io.ReadCloser
	rw      HTTPReaderWriter
	conn    *httpConnect
	release nativeTransportRelease
	closed  bool
}

func (s *httpFormatStream) Read(p []byte) (int, error) {
	if s.closed {
		return 0, errors.New("clickhouse: read on closed format stream")
	}
	return s.reader.Read(p)
}

func (s *httpFormatStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	discardAndClose(s.body)
	s.conn.compressionPool.Put(s.rw)
	s.release(s.conn, nil)
	return nil
}

func (h *httpConnect) queryFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, args ...any) (io.ReadCloser, error) {
	h.logger.Debug("HTTP format query", slog.String("sql", query), slog.String("format", formatName))
	options := queryOptions(ctx)
	query, err := bindQueryOrAppendParameters(true, &options, query, h.handshake.Timezone, args...)
	if err != nil {
		err = fmt.Errorf("bindQueryOrAppendParameters: %w", err)
		release(h, err)
		return nil, err
	}

	headers := make(map[string]string)
	switch h.compression {
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		headers["Accept-Encoding"] = h.compression.String()
	case CompressionZSTD, CompressionLZ4:
		// Native block compression wraps the response in ClickHouse block
		// framing, which would corrupt the raw format stream - skip it.
	}

	req, err := h.prepareRequest(ctx, query, &options, headers)
	if err != nil {
		release(h, err)
		return nil, err
	}
	// Override the connection-level default_format=Native: over HTTP the
	// server itself encodes the response in the requested format. A FORMAT
	// clause inside the query would take precedence - the documented contract
	// is to pass the format as the argument instead.
	q := req.URL.Query()
	q.Set("default_format", formatName)
	req.URL.RawQuery = q.Encode()

	res, err := h.executeRequest(req) //nolint:bodyclose // closed via httpFormatStream.Close
	if err != nil {
		release(h, err)
		return nil, err
	}

	rw := h.compressionPool.Get()
	reader, err := rw.NewReader(res)
	if err != nil {
		err = fmt.Errorf("NewReader: %w", err)
		discardAndClose(res.Body)
		h.compressionPool.Put(rw)
		release(h, err)
		return nil, err
	}

	return &httpFormatStream{
		reader:  newExceptionScanReader(reader),
		body:    res.Body,
		rw:      rw,
		conn:    h,
		release: release,
	}, nil
}

func (h *httpConnect) insertFormat(ctx context.Context, release nativeTransportRelease, formatName string, query string, data io.Reader) error {
	h.logger.Debug("HTTP format insert", slog.String("sql", query), slog.String("format", formatName))
	insertStmt, _, _, err := extractInsertQueryComponents(query)
	if err != nil {
		release(h, err)
		return err
	}

	options := queryOptions(ctx)
	headers := make(map[string]string)
	body := data
	switch h.compression {
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		headers["Content-Encoding"] = h.compression.String()
		rw := h.compressionPool.Get()
		defer h.compressionPool.Put(rw)
		pr, pw := io.Pipe()
		connWriter := rw.reset(pw)
		// Compresses the caller's payload into the request body. The goroutine
		// exits when data is exhausted, or when the HTTP client closes the
		// pipe reader on request failure and the writes start erroring.
		go func() {
			_, err := io.Copy(connWriter, data)
			if cErr := connWriter.Close(); err == nil {
				err = cErr
			}
			pw.CloseWithError(err)
		}()
		body = pr
	case CompressionZSTD, CompressionLZ4:
		// decompress=1 expects ClickHouse native block framing, which a raw
		// pre-formatted payload does not carry - send it uncompressed.
	}

	// The format argument is authoritative: any FORMAT clause in the original
	// query was stripped by extractInsertQueryComponents.
	options.settings["query"] = insertStmt + " FORMAT " + formatName
	headers["Content-Type"] = "application/octet-stream"

	res, err := h.sendStreamQuery(ctx, body, &options, headers) //nolint:bodyclose // false positive
	if err != nil {
		err = fmt.Errorf("insert %s: %w", formatName, err)
		release(h, err)
		return err
	}
	discardAndClose(res.Body)
	release(h, nil)
	return nil
}
