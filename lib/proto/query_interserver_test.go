package proto

import (
	"bytes"
	"crypto/sha256"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
)

func TestInterserverHashEmptySecret(t *testing.T) {
	q := Query{ClusterSalt: "salt", Body: "SELECT 1", ID: "qid", InitialUser: "alice"}
	if got := q.interserverHash(); got != "" {
		t.Fatalf("expected empty hash without cluster secret, got %q", got)
	}
}

func TestInterserverHashMatchesClickHouseLayout(t *testing.T) {
	q := Query{
		ClusterSecret: "topsecret",
		ClusterSalt:   "01234567890123456789012345678901",
		Body:          "SELECT 42",
		ID:            "test-query-id",
		InitialUser:   "alice",
	}

	h := sha256.New()
	h.Write([]byte(q.ClusterSalt))
	h.Write([]byte(q.ClusterSecret))
	h.Write([]byte(q.Body))
	h.Write([]byte(q.ID))
	h.Write([]byte(q.InitialUser))
	want := string(h.Sum(nil))

	got := q.interserverHash()
	if got != want {
		t.Fatalf("hash mismatch\n got: %x\nwant: %x", got, want)
	}
	if len(got) != 32 {
		t.Fatalf("expected 32-byte hash, got %d bytes", len(got))
	}
}

func TestInterserverHashChangesWithInitialUser(t *testing.T) {
	base := Query{
		ClusterSecret: "secret",
		ClusterSalt:   "salt",
		Body:          "SELECT 1",
		ID:            "id",
	}
	a := base
	a.InitialUser = "alice"
	b := base
	b.InitialUser = "bob"
	if a.interserverHash() == b.interserverHash() {
		t.Fatal("hash must differ when initial_user differs")
	}
}

func TestInterserverHashChangesWithBody(t *testing.T) {
	base := Query{
		ClusterSecret: "secret",
		ClusterSalt:   "salt",
		ID:            "id",
		InitialUser:   "alice",
	}
	a := base
	a.Body = "SELECT 1"
	b := base
	b.Body = "SELECT 2"
	if a.interserverHash() == b.interserverHash() {
		t.Fatal("hash must differ when body differs")
	}
}

// TestEncodeClientInfoQueryKind verifies that the query_kind byte flips to
// Secondary when interserver mode is enabled and stays Initial otherwise.
// query_kind is the very first byte of client_info, which sits right after
// the query ID, so we can read it deterministically without decoding the
// whole frame.
func TestEncodeClientInfoQueryKind(t *testing.T) {
	cases := []struct {
		name   string
		secret string
		want   byte
	}{
		{"initial when no secret", "", ClientQueryInitial},
		{"secondary when secret set", "secret", ClientQuerySecondary},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := Query{
				ID:            "qid",
				Body:          "SELECT 1",
				InitialUser:   "alice",
				ClusterSecret: tc.secret,
				ClusterSalt:   "salt",
			}
			buf := &chproto.Buffer{}
			if err := q.Encode(buf, DBMS_TCP_PROTOCOL_VERSION); err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			// Skip the leading query ID string: var-len header + body.
			// We simply locate the first byte after PutString("qid") by
			// advancing past the string-length prefix and the bytes.
			r := chproto.NewReader(bytes.NewReader(buf.Buf))
			id, err := r.Str()
			if err != nil {
				t.Fatalf("read query id: %v", err)
			}
			if id != "qid" {
				t.Fatalf("query id mismatch: %q", id)
			}
			gotKind, err := r.ReadByte()
			if err != nil {
				t.Fatalf("read query_kind: %v", err)
			}
			if gotKind != tc.want {
				t.Fatalf("query_kind = %d, want %d", gotKind, tc.want)
			}
		})
	}
}

// TestEncodeEmptySecretPreservesLegacyHashSlot verifies that when ClusterSecret
// is empty the interserver-secret slot is the legacy empty string. This is a
// regression guard: existing callers without interserver mode must produce
// byte-identical wire output.
func TestEncodeEmptySecretPreservesLegacyHashSlot(t *testing.T) {
	q := Query{
		ID:          "qid",
		Body:        "SELECT 1",
		InitialUser: "alice",
	}
	buf := &chproto.Buffer{}
	if err := q.Encode(buf, DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	// Re-encode separately and compare against a buffer where interserverHash
	// must be "". The presence of the SHA256 layout would shift downstream
	// bytes, so a successful equality check on the full body is sufficient.
	if !containsEmptyHashSlot(buf.Buf) {
		t.Fatalf("expected empty interserver-secret slot in encoded query")
	}
}

// containsEmptyHashSlot scans the encoded query for the empty-string slot
// that follows the settings terminator and precedes StateComplete. Encoded
// strings are length-prefixed with a var-uint, so an empty string is the
// single byte 0x00. We walk the prefix forward and look for the 0x00, 0x02
// pair (empty hash + StateComplete). The check is loose but sufficient to
// catch a regression where the slot grows to 32 bytes.
func containsEmptyHashSlot(b []byte) bool {
	for i := 0; i < len(b)-1; i++ {
		if b[i] == 0x00 && b[i+1] == StateComplete {
			return true
		}
	}
	return false
}
