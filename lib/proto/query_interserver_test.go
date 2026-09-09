package proto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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

	got := q.interserverHash()
	const want = "9da969adc18fb12f2a137d112599fc19afc0ba0d96d2a00b50e20a806a09b6b4"
	if gotHex := hex.EncodeToString([]byte(got)); gotHex != want {
		t.Fatalf("hash mismatch\n got: %s\nwant: %s", gotHex, want)
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

// TestEncodeEmptySecretUsesLegacyHashSlot verifies the exact encoded hash
// field rather than looking for an ambiguous byte sequence elsewhere in the
// frame. It also verifies that enabling signing adds exactly one SHA256 digest.
func TestEncodeEmptySecretUsesLegacyHashSlot(t *testing.T) {
	emptyQuery := Query{
		ID:          "qid",
		Body:        "SELECT 1",
		InitialUser: "alice",
	}
	signedQuery := emptyQuery
	signedQuery.ClusterSecret = "secret"
	signedQuery.ClusterSalt = "salt"

	emptyBuf := &chproto.Buffer{}
	if err := emptyQuery.Encode(emptyBuf, DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode empty-secret query: %v", err)
	}
	signedBuf := &chproto.Buffer{}
	if err := signedQuery.Encode(signedBuf, DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode signed query: %v", err)
	}

	if got, want := len(signedBuf.Buf)-len(emptyBuf.Buf), sha256.Size; got != want {
		t.Fatalf("signed frame length delta = %d, want %d", got, want)
	}
	assertEncodedInterserverHash(t, emptyQuery, emptyBuf.Buf, "")
	assertEncodedInterserverHash(t, signedQuery, signedBuf.Buf, signedQuery.interserverHash())
}

func assertEncodedInterserverHash(t *testing.T, q Query, encoded []byte, want string) {
	t.Helper()
	prefix := &chproto.Buffer{}
	prefix.PutString(q.ID)
	if err := q.encodeClientInfo(prefix, DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode client info: %v", err)
	}
	if err := q.Settings.Encode(prefix, DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode settings: %v", err)
	}
	prefix.PutString("")

	r := chproto.NewReader(bytes.NewReader(encoded[len(prefix.Buf):]))
	got, err := r.Str()
	if err != nil {
		t.Fatalf("read interserver hash: %v", err)
	}
	if got != want {
		t.Fatalf("interserver hash = %x, want %x", got, want)
	}
	state, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read query state: %v", err)
	}
	if state != StateComplete {
		t.Fatalf("query state = %d, want %d", state, StateComplete)
	}
}
