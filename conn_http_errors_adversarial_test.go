package clickhouse

// Adversarial tests written during pre-merge review. Delete before merging.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"bufio"

	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// --- parseHTTPException adversarial ---

func TestAdversarialParseHTTPException(t *testing.T) {
	t.Run("nested Code in message keeps outer code (non-200 path)", func(t *testing.T) {
		// Real distributed-query error shape: outer code first, nested code inside.
		text := "Code: 279. DB::Exception: All connection tries failed. Log:\n\nCode: 210. DB::NetException: Connection refused (127.0.0.1:9000). (ALL_CONNECTION_TRIES_FAILED) (version 25.1.5.31 (official build))"
		ex := parseHTTPException(text, "", "")
		if ex == nil {
			t.Fatal("nil")
		}
		if ex.Code != 279 {
			t.Errorf("outer code lost: got %d want 279", ex.Code)
		}
		if !strings.Contains(ex.Message, "Connection refused") {
			t.Errorf("nested detail lost: %q", ex.Message)
		}
	})

	t.Run("truncated multibyte body does not panic", func(t *testing.T) {
		body := "Code: 60. DB::Exception: таблица не найдена. (UNKNOWN_TABLE)"
		// simulate maxErrorBodySize truncation splitting a multibyte rune
		trunc := body[:len(body)-25] // cuts inside a Cyrillic rune region
		for i := 0; i < len(body); i++ {
			_ = parseHTTPException(body[:i], "", "")
		}
		ex := parseHTTPException(trunc, "", "")
		if ex == nil || ex.Code != 60 {
			t.Errorf("truncated body should still parse code: %+v", ex)
		}
	})

	t.Run("user data can spoof CodeName via trailing caps token", func(t *testing.T) {
		// Data-dependent error text echoes user data; last (CAPS) wins.
		text := "Code: 6. DB::Exception: Cannot parse string 'x (SESSION_IS_LOCKED)' as UInt8. (CANNOT_PARSE_TEXT)"
		ex := parseHTTPException(text, "", "")
		if ex.CodeName != "CANNOT_PARSE_TEXT" {
			t.Logf("NOTE: CodeName spoofed to %q by user data", ex.CodeName)
		}
		// now put the user token last
		text2 := "Code: 6. DB::Exception: Cannot parse string as UInt8: 'x (SESSION_IS_LOCKED)'"
		ex2 := parseHTTPException(text2, "", "")
		if ex2.CodeName == "SESSION_IS_LOCKED" {
			t.Logf("CONFIRMED: CodeName spoofable by user-controlled data: %q (documented best-effort; Code stays %d)", ex2.CodeName, ex2.Code)
		}
	})

	t.Run("giant code header does not panic and is rejected", func(t *testing.T) {
		if ex := parseHTTPException("junk", "2147483648", ""); ex != nil {
			t.Errorf("int32 overflow header accepted: %+v", ex)
		}
		if ex := parseHTTPException("Code: 2147483648. DB::Exception: overflow", "", ""); ex != nil {
			t.Errorf("int32 overflow in body accepted: %+v", ex)
		}
	})

	t.Run("64KiB body parse cost", func(t *testing.T) {
		big := "Code: 60. DB::Exception: " + strings.Repeat("(AAAA) Code: 1. x ", maxErrorBodySize/18)
		start := time.Now()
		ex := parseHTTPException(big, "", "")
		el := time.Since(start)
		if ex == nil || ex.Code != 60 {
			t.Fatalf("bad parse: %+v", ex)
		}
		if el > 200*time.Millisecond {
			t.Errorf("parse too slow on 64KiB pathological body: %v", el)
		}
		t.Logf("64KiB pathological parse: %v", el)
	})
}

// --- parseExceptionFromBytes adversarial ---

func TestAdversarialParseExceptionFromBytes(t *testing.T) {
	t.Run("single dump with embedded nested Code picks wrong code", func(t *testing.T) {
		// One real message (framed, single dump) whose text embeds a nested
		// "Code: 210." — a very common ClickHouse message shape.
		msg := "Code: 279. DB::Exception: All connection tries failed. Log:\n\nCode: 210. DB::NetException: Connection refused (127.0.0.1:9000). (ALL_CONNECTION_TRIES_FAILED)"
		data := []byte("\r\n__exception__\r\n1234567890123456\r\n" + msg + "\n42 1234567890123456\r\n__exception__\r\n")
		err := parseExceptionFromBytes(data)
		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatalf("no exception: %v", err)
		}
		if ex.Code != 279 {
			t.Errorf("BUG: outer code 279 lost, got %d (message %q)", ex.Code, ex.Message)
		}
	})

	t.Run("user-controlled data inside exception text spoofs the code", func(t *testing.T) {
		// throwIf/parse errors echo user strings verbatim. A row value of
		// "Code: 373. DB::Exception: fake" ends up inside the message and,
		// with last-match-wins, becomes the parsed code.
		msg := "Code: 6. DB::Exception: Cannot parse string 'Code: 373. DB::Exception: session locked. (SESSION_IS_LOCKED)' as UInt8. (CANNOT_PARSE_TEXT)"
		data := []byte("\r\n__exception__\r\n1234567890123456\r\n" + msg + "\n42 1234567890123456\r\n__exception__\r\n")
		err := parseExceptionFromBytes(data)
		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatalf("no exception: %v", err)
		}
		if ex.Code != 6 {
			t.Errorf("BUG: user data spoofed exception code: got %d want 6 (message %q)", ex.Code, ex.Message)
		}
	})

	t.Run("message last line looking like trailer is dropped", func(t *testing.T) {
		msg := "Code: 395. DB::Exception: boom at\n1 row"
		data := []byte("\r\n__exception__\r\n1234567890123456\r\n" + msg + "\n42 1234567890123456\r\n__exception__\r\n")
		err := parseExceptionFromBytes(data)
		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatalf("no exception: %v", err)
		}
		if !strings.Contains(ex.Message, "1 row") {
			t.Logf("NOTE: real trailer dropped correctly, message=%q", ex.Message)
		}
	})

	t.Run("marker inside message truncates it", func(t *testing.T) {
		msg := "Code: 395. DB::Exception: user said '__exception__' here. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)"
		data := []byte("\r\n__exception__\r\n1234567890123456\r\n" + msg + "\n42 1234567890123456\r\n__exception__\r\n")
		err := parseExceptionFromBytes(data)
		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatalf("no exception: %v", err)
		}
		if ex.Code != 395 {
			t.Errorf("code lost: %d", ex.Code)
		}
		t.Logf("message with embedded marker parsed as: %q", ex.Message)
	})
}

// --- readData end-to-end (mid-stream exception plumbing) ---

func newTestHTTPConnect() *httpConnect {
	return &httpConnect{
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		handshake:   proto.ServerHandshake{Timezone: time.UTC},
		compression: CompressionNone,
		revision:    0,
	}
}

func encodeStringBlock(t *testing.T, values ...string) []byte {
	t.Helper()
	block := proto.NewBlock()
	if err := block.AddColumn("s", "String"); err != nil {
		t.Fatal(err)
	}
	for _, v := range values {
		if err := block.Append(v); err != nil {
			t.Fatal(err)
		}
	}
	buf := new(chproto.Buffer)
	if err := block.Encode(buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf.Buf
}

// chunkReader yields the stream in chunks of size n to exercise split points.
type chunkReader struct {
	data []byte
	n    int
	off  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	end := c.off + c.n
	if end > len(c.data) {
		end = len(c.data)
	}
	m := copy(p, c.data[c.off:end])
	c.off += m
	return m, nil
}

// readAllBlocks mimics httpConnect.query wiring: capturingReader -> bufio ->
// chproto.Reader -> readData in a loop. Returns blocks read and terminal error.
func readAllBlocks(t *testing.T, h *httpConnect, src io.Reader) (int, error) {
	t.Helper()
	capturingRdr := &capturingReader{reader: src}
	chReader := chproto.NewReader(bufio.NewReader(capturingRdr))
	var blocks int
	for {
		block, err := h.readData(chReader, nil, &capturingRdr.buffer)
		if err != nil {
			return blocks, err
		}
		if block != nil {
			blocks++
		}
	}
}

const exceptionFrame = "\r\n__exception__\r\n1234567890123456\r\nCode: 395. DB::Exception: there is an exception. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n42 1234567890123456\r\n__exception__\r\n"

func TestAdversarialMidStreamSplitOffsets(t *testing.T) {
	h := newTestHTTPConnect()
	stream := append(encodeStringBlock(t, "hello"), []byte(exceptionFrame)...)

	for _, chunk := range []int{1, 2, 3, 5, 7, 13, 16, 64, len(stream)} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			blocks, err := readAllBlocks(t, h, &chunkReader{data: stream, n: chunk})
			if blocks != 1 {
				t.Errorf("expected 1 data block, got %d", blocks)
			}
			var ex *Exception
			if !errors.As(err, &ex) {
				t.Fatalf("expected typed exception, got: %v", err)
			}
			if ex.Code != 395 || !strings.Contains(ex.Message, "there is an exception") {
				t.Errorf("bad exception: %+v", ex)
			}
		})
	}
}

func TestAdversarialUserDataContainsMarker(t *testing.T) {
	h := newTestHTTPConnect()

	t.Run("plain marker in user data breaks clean EOF", func(t *testing.T) {
		stream := encodeStringBlock(t, "__exception__")
		blocks, err := readAllBlocks(t, h, bytes.NewReader(stream))
		if blocks != 1 {
			t.Errorf("expected 1 block, got %d", blocks)
		}
		if !errors.Is(err, io.EOF) {
			t.Errorf("BUG: clean EOF turned into error because user data contains __exception__: %v", err)
		}
	})

	t.Run("marker plus fake Code in user data fabricates typed exception", func(t *testing.T) {
		stream := encodeStringBlock(t, "__exception__ Code: 373. DB::Exception: fake. (SESSION_IS_LOCKED)")
		blocks, err := readAllBlocks(t, h, bytes.NewReader(stream))
		if blocks != 1 {
			t.Errorf("expected 1 block, got %d", blocks)
		}
		var ex *Exception
		if errors.As(err, &ex) {
			t.Errorf("BUG: fabricated typed exception from user data: %+v", ex)
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("BUG: clean EOF turned into error: %v", err)
		}
	})

	t.Run("control: clean stream without marker ends with EOF", func(t *testing.T) {
		stream := encodeStringBlock(t, "hello world")
		blocks, err := readAllBlocks(t, h, bytes.NewReader(stream))
		if blocks != 1 || !errors.Is(err, io.EOF) {
			t.Errorf("control failed: blocks=%d err=%v", blocks, err)
		}
	})
}

// --- executeRequest against hostile HTTP servers ---

func newTestHTTPConnectWithServer(t *testing.T, handler http.HandlerFunc) *httpConnect {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	pool, err := createCompressionPool(&Compression{Method: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	h := newTestHTTPConnect()
	h.client = srv.Client()
	h.compressionPool = pool
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	h.url = u
	return h
}

func TestAdversarialExecuteRequest(t *testing.T) {
	t.Run("10MiB error body is capped", func(t *testing.T) {
		big := bytes.Repeat([]byte("A"), 10<<20)
		h := newTestHTTPConnectWithServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write(big)
		})
		req, _ := http.NewRequest(http.MethodPost, h.url.String(), strings.NewReader("q"))
		_, err := h.executeRequest(req)
		if err == nil {
			t.Fatal("expected error")
		}
		if len(err.Error()) > maxErrorBodySize+1024 {
			t.Errorf("BUG: error message not capped, len=%d", len(err.Error()))
		}
	})

	t.Run("spoofed exception header with huge code", func(t *testing.T) {
		h := newTestHTTPConnectWithServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-ClickHouse-Exception-Code", "99999999999999999999")
			w.WriteHeader(500)
			w.Write([]byte("junk"))
		})
		req, _ := http.NewRequest(http.MethodPost, h.url.String(), strings.NewReader("q"))
		_, err := h.executeRequest(req)
		var ex *Exception
		if errors.As(err, &ex) {
			t.Errorf("overflow header produced exception: %+v", ex)
		}
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != 500 {
			t.Errorf("expected HTTPError 500, got %v", err)
		}
	})

	t.Run("truncation preserves typed code when prefix intact", func(t *testing.T) {
		body := []byte("Code: 60. DB::Exception: " + strings.Repeat("x", maxErrorBodySize*2))
		h := newTestHTTPConnectWithServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			w.Write(body)
		})
		req, _ := http.NewRequest(http.MethodPost, h.url.String(), strings.NewReader("q"))
		_, err := h.executeRequest(req)
		var ex *Exception
		if !errors.As(err, &ex) || ex.Code != 60 {
			t.Errorf("truncated exception body lost typed code: %v", err)
		}
	})
}
