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

// maxCloseDrainBytes bounds how much unread body Close pulls before giving
// up on keep-alive reuse of the HTTP connection. The common case is a stream
// already read to EOF (nothing left to drain) or a short tail; past the
// bound, losing the connection is cheaper than transferring a result the
// caller has abandoned.
const maxCloseDrainBytes = 64 << 10 // 64 KiB

// exceptionFrame is basically exceptionMarker (`__exception__`) + CRLF.
// This is exactly how ClickHouse server writes it on the write. It is there to differentiate
// say normal text `__exception__` in the body.
var exceptionFrame = []byte(exceptionMarker + "\r\n")

// exceptionScanReader is used to extract full exception body during mid-stream exception.
// It uses exceptionFrame instead of just plain exceptionMarker to discover the exception.
//
// A frame candidate is treated as a genuine exception only if the marker is
// followed by the per-response random tag the server announced in the
// X-ClickHouse-Exception-Tag HTTP header (tag). On older servers that send no tag header,
// tag is empty and the marker alone is trusted (best-effort).
type exceptionScanReader struct {
	// src is the upstream reader
	src io.Reader

	// tag is from X-ClickHouse-Exception-Tag HTTP header;
	// empty disables validation (older version)
	tag string

	// buf is the scratch buffer (max exceptionScanLimit)
	buf []byte

	// pending is every byte that is not returned to the caller.
	// it may or may not contain exception.
	pending []byte

	// pending[:scanOffset] holds plain text and are not exception candidates.
	scanOffset int

	// terminal error: parsed exception or upstream read error
	err error

	// srcDone is true once the upstream reader is exhausted (EOF or error).
	// no more input is coming
	srcDone bool
}

func newExceptionScanReader(src io.Reader, tag string) *exceptionScanReader {
	return &exceptionScanReader{src: src, tag: tag, buf: make([]byte, exceptionScanLimit)}
}

// waitEndOfQueryEnabled reports whether the caller opted into all-or-nothing
// semantics via the wait_end_of_query setting. With it, the server buffers the
// complete result before responding: a failure surfaces as a non-200 status
// before any body byte and no in-band exception marker can appear, so the
// marker scan is skipped entirely. A per-query setting takes precedence over
// the connection-level default from Options.Settings (baked into h.url at
// dial time).
func (h *httpConnect) waitEndOfQueryEnabled(settings Settings) bool {
	if _, ok := settings["wait_end_of_query"]; ok {
		return settingEnabled(settings, "wait_end_of_query")
	}
	return h.waitEndOfQuery
}

// settingEnabled reports whether the named setting is present in settings and
// set to a truthy value ("1" or "true").
func settingEnabled(settings Settings, name string) bool {
	v, ok := settings[name]
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
			if r.scanOffset -= n; r.scanOffset < 0 {
				r.scanOffset = 0
			}
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
		}
		if err != nil {
			r.srcDone = true
			if !errors.Is(err, io.EOF) && r.err == nil {
				r.err = err
			}
		}
		if n > 0 || r.srcDone {
			r.scan()
		}
	}
}

// scan classifies every complete frame candidate in pending. Candidates whose
// tag bytes have not arrived yet stay undecided: scanOffset keeps pointing
// before them and safeLen holds those bytes back until more input (or the end
// of the stream) settles the question.
func (r *exceptionScanReader) scan() {
	for {
		i := bytes.Index(r.pending[r.scanOffset:], exceptionFrame)
		if i < 0 {
			// No complete frame exists at or after scanOffset, so every byte
			// except the split-frame holdback tail is decided data; advancing
			// scanOffset keeps safeLen bounded per Read instead of rescanning
			// all of pending
			if cleared := len(r.pending) - (len(exceptionFrame) - 1); cleared > r.scanOffset {
				r.scanOffset = cleared
			}
			return
		}
		i += r.scanOffset
		if r.tag == "" {
			// Old servers (<= 25.8) announce no tag: the marker alone has to
			// be trusted.
			r.captureException(i)
			return
		}
		tagStart := i + len(exceptionFrame)
		tagEnd := tagStart + len(r.tag) + 2 // tag + CRLF
		if len(r.pending) < tagEnd {
			if !r.srcDone {
				// Undecided: wait for the tag bytes. Everything before the
				// candidate is decided data, so park scanOffset right at it -
				// safeLen then re-examines only the candidate, not the data
				// preceding it, while the caller drains that data.
				r.scanOffset = i
				return
			}
			// The stream ended inside the candidate - a genuine frame cannot
			// be truncated here, so these are data bytes.
			r.scanOffset = i + 1
			continue
		}
		if string(r.pending[tagStart:tagStart+len(r.tag)]) == r.tag &&
			bytes.Equal(r.pending[tagStart+len(r.tag):tagEnd], []byte("\r\n")) {
			r.captureException(i)
			return
		}
		// Marker bytes that happen to appear in the result data.
		r.scanOffset = i + 1
	}
}

// safeLen returns how many pending bytes can be served without risking that
// their tail is the beginning of an exception frame split across reads.
// Candidates already dismissed by scan (below scanOffset) are plain data and
// never held back.
func (r *exceptionScanReader) safeLen() int {
	if r.srcDone || r.err != nil {
		return len(r.pending)
	}
	tail := r.pending[r.scanOffset:]
	if i := bytes.Index(tail, exceptionFrame); i >= 0 {
		// A complete marker whose tag bytes are still in flight: serve
		// everything before it, hold the candidate.
		return r.scanOffset + i
	}
	holdback := len(exceptionFrame) - 1
	if holdback > len(tail) {
		holdback = len(tail)
	}
	for k := holdback; k > 0; k-- {
		if bytes.Equal(tail[len(tail)-k:], exceptionFrame[:k]) {
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

	// r.src is the live response body: a read failure here means the
	// exception block itself is truncated, which the caller must see rather
	// than a silently degraded message.
	rest, readErr := io.ReadAll(io.LimitReader(r.src, exceptionScanLimit))
	exc = append(exc, rest...)
	r.err = parseExceptionFromBytes(exc)
	if readErr != nil {
		r.err = fmt.Errorf("%w (exception block truncated: %v)", r.err, readErr)
	}
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
	// Bounded drain: reuse the connection when the stream was (nearly)
	// exhausted, but never block a caller that abandoned a large result -
	// closing an undrained body forfeits the connection, which is cheaper.
	_, _ = io.CopyN(io.Discard, s.body, maxCloseDrainBytes)
	_ = s.body.Close()
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

	// With http_write_exception_in_output_format the server embeds a
	// mid-stream failure inside the payload of formats that can carry it
	// (JSON, XML, ...) and ends the stream cleanly - the caller would get
	// partial data and no error. Pin it to 0 so failures always arrive as an
	// "__exception__" block the scan below can surface, regardless of format.
	// An explicit caller setting (per-query or connection-level) wins.
	s := "http_write_exception_in_output_format"
	if _, ok := options.settings[s]; !ok {
		if _, ok := h.opt.Settings[s]; !ok {
			options.settings[s] = 0
		}
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
	// streaming responses need the in-band exception scan, validated against
	// the per-response tag the server announced in the headers.
	//
	// The scan sits on the decompressed side deliberately: servers with
	// tagged exception framing write the "__exception__" block through the
	// HTTP compression buffer, so with Content-Encoding the marker only
	// exists after decompression. Older servers (<= 25.8) write no block at
	// all on a compressed response - they abort the stream, and the
	// decompressor's truncation error is all there is to surface
	// (TestFormatMidStreamExceptionCompressed pins both behaviours).
	stream := reader
	if !h.waitEndOfQueryEnabled(options.settings) {
		stream = newExceptionScanReader(reader, res.Header.Get(exceptionTagHeader))
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
		// Client-side parse failure: the connection is healthy and unused,
		// releasing it with the error would needlessly close it.
		release(h, nil)
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
	// A 200 status is not yet success: a failure after the server flushed its
	// headers arrives in-band, in the response body.
	if err := h.insertResponseError(res); err != nil {
		err = fmt.Errorf("insert %s: %w", formatName, err)
		release(h, err)
		return err
	}
	release(h, nil)
	return nil
}
