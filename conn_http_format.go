package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// exceptionScanLimit represents how much to scan once the exception marker
// is found during mid-stream exception body
const exceptionScanLimit = 32 << 10 // 32 KiB

// exceptionFrame is basically exceptionMarker (`__exception__`) + CRLF.
// This is exactly how ClickHouse server writes it on the write. It is there to differentiate
// say normal text `__exception__` in the body.
var exceptionFrame = []byte(exceptionMarker + "\r\n")

// exceptionScanReader is used to extract full exception body during mid-stream exception.
// It uses exceptionFrame instead of just plain exceptionMarker to discover the exception.
type exceptionScanReader struct {
	src     io.Reader
	buf     []byte
	pending []byte
	err     error // terminal error: parsed exception or upstream read error
	srcDone bool
}

func newExceptionScanReader(src io.Reader) *exceptionScanReader {
	return &exceptionScanReader{src: src, buf: make([]byte, exceptionScanLimit)}
}

// waitEndOfQueryEnabled reports whether the caller opted into all-or-nothing
// semantics via the wait_end_of_query setting. With it, the server buffers the
// complete result before responding: a failure surfaces as a non-200 status
// before any body byte and no in-band exception marker can appear, so the
// marker scan is skipped entirely.
func waitEndOfQueryEnabled(settings Settings) bool {
	v, ok := settings["wait_end_of_query"]
	if !ok {
		return false
	}
	if cv, ok := v.(CustomSetting); ok {
		v = cv.Value
	}
	switch fmt.Sprint(v) {
	case "1", "true":
		return true
	default:
		return false
	}
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
			if i := bytes.Index(r.pending, exceptionFrame); i >= 0 {
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
// their tail is the beginning of an exception frame split across reads.
func (r *exceptionScanReader) safeLen() int {
	if r.srcDone || r.err != nil {
		return len(r.pending)
	}
	holdback := len(exceptionFrame) - 1
	if holdback > len(r.pending) {
		holdback = len(r.pending)
	}
	for k := holdback; k > 0; k-- {
		if bytes.Equal(r.pending[len(r.pending)-k:], exceptionFrame[:k]) {
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

	// safety: all in-memory pre-allocated buffer reads. skipping error
	rest, _ := io.ReadAll(io.LimitReader(r.src, exceptionScanLimit))
	exc = append(exc, rest...)
	r.err = parseExceptionFromBytes(exc)
	r.srcDone = true
}

// lazyReadCloser defers side effects (begin) until the wrapped reader is first
// read. It lets insertFormat postpone spawning the compression copy until the
// HTTP client actually pulls the request body.
type lazyReadCloser struct {
	begin func()
	rc    io.ReadCloser
}

func (l *lazyReadCloser) Read(p []byte) (int, error) {
	l.begin() // idempotent: guarded by a sync.Once
	return l.rc.Read(p)
}

func (l *lazyReadCloser) Close() error { return l.rc.Close() }

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

	// With wait_end_of_query the server has already buffered the full result
	// and failures arrived as a non-200 above - serve the body as-is. Only
	// streaming responses need the best-effort in-band marker scan.
	stream := reader
	if !waitEndOfQueryEnabled(options.settings) {
		stream = newExceptionScanReader(reader)
	}

	return &httpFormatStream{
		reader:  stream,
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
		pr, pw := io.Pipe()
		connWriter := rw.reset(pw)

		// Compress lazily: the copy goroutine is spawned only when the HTTP
		// client first reads the request body. A request that fails before
		// reading the body (connection refused, DNS, TLS, ...) therefore never
		// touches the caller's data reader, so it cannot strand a copy blocked
		// on a slow or stalled source.
		var startCopy sync.Once
		done := make(chan struct{})
		spawn := func() {
			go func() {
				defer close(done)
				_, err := io.Copy(connWriter, data)
				if cErr := connWriter.Close(); err == nil {
					err = cErr
				}
				pw.CloseWithError(err)
			}()
		}
		body = &lazyReadCloser{begin: func() { startCopy.Do(spawn) }, rc: pr}

		// Return the pooled compressor only after the copy goroutine has
		// finished - never while it is still writing - so a failed or short
		// request cannot hand an in-use writer to another connection (a data
		// race that would corrupt a later request body). Reclaiming happens off
		// the calling goroutine so a stalled data source never blocks the
		// caller; if the copy never started, the writer is reusable at once.
		defer func() {
			pr.CloseWithError(io.ErrClosedPipe) // unblock a copy parked on pw.Write
			neverStarted := false
			startCopy.Do(func() { neverStarted = true })
			if neverStarted {
				h.compressionPool.Put(rw)
				return
			}
			go func() {
				<-done
				h.compressionPool.Put(rw)
			}()
		}()
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
