package churl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Parse is a fork of net/url.Parse modified to accept multiple comma
// separated hosts in the authority (e.g. "host1:9000,host2:9000") for
// ClickHouse HA DSNs — something the stdlib parser stopped allowing after
// Go 1.26 (golang.org/issue/75859). clickhouse_options.go's DSN parser is
// the sole caller, so a regression here breaks every multi-host connection
// string silently or loudly, depending on the bug.

func TestParseHosts(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"basic DSN", "clickhouse://user:pass@localhost:9000/default?dial_timeout=1s", "localhost:9000"},
		// This is the entire reason churl exists: net/url.Parse (>= Go
		// 1.26) rejects commas in the host part. clickhouse-go relies on
		// getting the raw comma-separated host string back intact so it
		// can split it into individual addresses itself (see
		// Options.fromDSN).
		{"multi-host with ports", "clickhouse://host1:9000,host2:9001,host3:9000/db", "host1:9000,host2:9001,host3:9000"},
		{"multi-host without ports", "clickhouse://host1,host2,host3/db", "host1,host2,host3"},
		{"multi-host with auth and query", "clickhouse://user:pass@host1:9000,host2:9000/default?secure=true", "host1:9000,host2:9000"},
		{"IPv6 with port", "clickhouse://[::1]:9000/db", "[::1]:9000"},
		{"IPv6 without port", "clickhouse://[::1]/db", "[::1]"},
		// RFC 6874: %25 introduces a zone identifier for a link-local
		// address, e.g. "fe80::1%en0". The zone itself may use its own
		// %-escaping rules.
		{"IPv6 with zone identifier", "clickhouse://[fe80::1%25en0]:9000/db", "[fe80::1%en0]:9000"},
		{"empty host with scheme (triple slash)", "clickhouse:///db", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, u.Host)
		})
	}
}

func TestParseUserinfo(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		wantUser    string
		wantPass    string
		wantHasPass bool
	}{
		{"username and password", "clickhouse://user:pass@localhost:9000/default", "user", "pass", true},
		// Documented quirk in parseAuthority/validUserinfo: '@' is
		// technically a delimiter between userinfo and host, but is
		// tolerated inside userinfo for compatibility
		// (see https://go.dev/issue/3439). The rightmost '@' is treated as
		// the real delimiter.
		{"@ inside password", "http://username:p@ssword@example.com/db", "username", "p@ssword", true},
		// validUserinfo has separate branches for A-Z, a-z, and 0-9.
		{"uppercase and digits", "clickhouse://User123:Pass456@host/db", "User123", "Pass456", true},
		{"username without password", "clickhouse://justuser@host/db", "justuser", "", false},
		// '+' is only special (decodes to space) in encodeQueryComponent
		// mode; in userinfo it stays literal even when the same string
		// also contains a "%XX" escape that forces the unescape rebuild
		// path to run.
		{"literal + alongside percent escape", "clickhouse://a+b%20c:pass@host/db", "a+b c", "pass", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			require.NotNil(t, u.User)
			assert.Equal(t, tc.wantUser, u.User.Username())
			pass, ok := u.User.Password()
			assert.Equal(t, tc.wantHasPass, ok)
			assert.Equal(t, tc.wantPass, pass)
		})
	}
}

func TestParseSchemeAndPath(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantScheme string
		wantPath   string
		wantOpaque string
	}{
		// A bare "host:port" with no "//" is parsed as an opaque
		// scheme:path, mirroring net/url's own behavior for e.g. "mailto:"
		// style URLs. This documents that DSNs handed to fromDSN must
		// include the "clickhouse://" (or "http(s)://") prefix or they
		// will not parse as a Host at all.
		{"bare host:port has no authority, becomes opaque", "localhost:9000/db", "localhost", "", "9000/db"},
		// getScheme bails out of scheme detection as soon as it sees a
		// character that can't start or continue one; the whole string is
		// then treated as a relative path instead of erroring.
		{"leading character invalid for a scheme", "@foo/bar", "", "@foo/bar", ""},
		// A string made entirely of scheme-legal letters but with no ':'
		// anywhere falls through getScheme's loop with no scheme found.
		{"letters only, no colon anywhere", "abcdef", "", "abcdef", ""},
		{"wildcard path", "*", "", "*", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantScheme, u.Scheme)
			assert.Equal(t, tc.wantPath, u.Path)
			assert.Equal(t, tc.wantOpaque, u.Opaque)
			assert.Empty(t, u.Host)
		})
	}
}

func TestParsePathEscaping(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		wantPath    string
		wantRawPath string
	}{
		// A decoded space re-escapes back to "%20" by default (space always
		// needs escaping in a path), so the round-trip is identical and
		// RawPath is left empty.
		{"percent-encoded space", "clickhouse://host/my%20db", "/my db", ""},
		// "%2F" decodes to '/', but re-escaping a decoded '/' for
		// encodePath does not re-add the percent-encoding (a literal '/'
		// is valid in a path), so the default escaping doesn't reproduce
		// the original text and RawPath must be kept to preserve it.
		{"percent-encoded slash forces RawPath", "clickhouse://host/foo%2Fbar", "/foo/bar", "/foo%2Fbar"},
		// ishex/unhex each branch separately on lowercase vs uppercase hex
		// digits; exercise the lowercase side ('e' in "%7e" decodes to
		// '~', an unreserved mark character that doesn't re-escape by
		// default, so RawPath is kept too.
		{"lowercase hex escape", "clickhouse://host/%7e", "/~", "/%7e"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPath, u.Path)
			assert.Equal(t, tc.wantRawPath, u.RawPath)
		})
	}
}

func TestParseFragment(t *testing.T) {
	cases := []struct {
		name            string
		in              string
		wantFragment    string
		wantRawFragment string
		wantRawQuery    string
	}{
		{"fragment is split off from the query", "clickhouse://host/db?x=1#section", "section", "", "x=1"},
		// setFragment only populates RawFragment when the default
		// re-escaping of the decoded fragment would not reproduce the
		// original text byte-for-byte (here, a literal space needs
		// escaping to round-trip).
		{"literal space forces RawFragment", "clickhouse://host/db#a b", "a b", "a b", ""},
		// RFC 3986 §4.1 allows the full reserved set unescaped in a
		// fragment; churl's shouldEscape mirrors that for encodeFragment.
		{"reserved characters stay unescaped", "clickhouse://host/db#a=b&c", "a=b&c", "", ""},
		{"sub-delims stay unescaped", "clickhouse://host/db#a(b)*c!d", "a(b)*c!d", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.wantFragment, u.Fragment)
			assert.Equal(t, tc.wantRawFragment, u.RawFragment)
			assert.Equal(t, tc.wantRawQuery, u.RawQuery)
		})
	}
}

func TestParseOmitHost(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		wantOmitHost bool
	}{
		{"triple slash: explicit empty authority", "clickhouse:///db", false},
		// A single slash after the scheme (no "//" authority marker at
		// all) sets OmitHost, distinct from an explicit-but-empty "///"
		// authority.
		{"single slash: no authority marker at all", "clickhouse:/db", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := Parse(tc.in)
			require.NoError(t, err)
			assert.Empty(t, u.Host)
			assert.Equal(t, tc.wantOmitHost, u.OmitHost)
			assert.Equal(t, "/db", u.Path)
		})
	}
}

func TestParseForceQuery(t *testing.T) {
	u, err := Parse("clickhouse://host/db?")
	require.NoError(t, err)
	assert.True(t, u.ForceQuery)
	assert.Empty(t, u.RawQuery)
}

func TestParseQueryParametersWithHAHosts(t *testing.T) {
	u, err := Parse("clickhouse://host1:9440,host2:9440/default?secure=true&skip_verify=true")
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "true", q.Get("secure"))
	assert.Equal(t, "true", q.Get("skip_verify"))
}

func TestEscapeLongStringExceedsStackBuffer(t *testing.T) {
	// escape() tries to use a fixed 64-byte stack buffer first and falls
	// back to a heap allocation once the escaped output would be longer
	// than that. Force the fallback with a fragment long enough that its
	// escaped form (3 bytes per escaped char) exceeds 64 bytes.
	frag := strings.Repeat(" ", 30) // each space becomes "%20" -> 90 bytes
	u, err := Parse("clickhouse://host/db#" + frag)
	require.NoError(t, err)
	assert.Equal(t, frag, u.Fragment)
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		errMsg string // substring expected in the error; "" only checks non-nil
	}{
		{"malformed percent-encoding in fragment", "clickhouse://host/db#%zz", ""},
		// With no scheme and no leading "/", a colon in the first path
		// segment is ambiguous with a scheme separator (RFC 3986 §3.3) and
		// must be rejected rather than silently treated as part of the path.
		{"first path segment with colon and no scheme", "0:foo/bar", "first path segment in URL cannot contain colon"},
		{"userinfo contains an invalid character", "clickhouse://user name@host/db", "invalid userinfo"},
		{"username has malformed percent-encoding, with password", "clickhouse://%zz:pass@host/db", ""},
		{"password has malformed percent-encoding", "clickhouse://user:%zz@host/db", ""},
		{"username has malformed percent-encoding, no password", "clickhouse://%zz@host/db", ""},
		{"IPv6 host missing closing bracket", "clickhouse://[::1/db", "missing ']' in host"},
		{"IPv6 host has an invalid port", "clickhouse://[::1]:abc/db", "invalid port"},
		{"IPv6 hostname has malformed percent-encoding, no zone", "clickhouse://[fe80::1%zz]:9000/db", ""},
		// Inside a zone identifier, only "%25" itself and escapes that
		// decode to characters already valid unescaped in a host are
		// allowed. "%2F" decodes to '/', which must stay escaped.
		{"IPv6 zone rejects a disallowed escape", "clickhouse://[fe80::1%25%2F]:9000/db", ""},
		// The portion of the bracketed hostname before the "%25" zone
		// marker is unescaped separately from the zone itself.
		{"IPv6 zone's host-part has malformed percent-encoding", "clickhouse://[%zz%25en0]:9000/db", ""},
		{"IPv6 literal is not a valid address", "clickhouse://[not-an-ip]:9000/db", "invalid host"},
		// Per RFC 3986, only IPv6 addresses may be bracketed.
		{"IPv6 brackets around an IPv4 literal are rejected", "clickhouse://[127.0.0.1]:9000/db", "invalid IP-literal"},
		{"IPv6 host has trailing garbage after the bracket", "clickhouse://[::1]xyz/db", "invalid port"},
		{"plain host has malformed percent-encoding", "clickhouse://%zz/db", ""},
		// Per RFC 3986 §3.2.2, %-encoding in a plain host component is
		// only meaningful for non-ASCII bytes; encoding an ASCII byte
		// below 0x80 (other than via the reserved "%25") is rejected even
		// though "%01" is syntactically well-formed hex.
		{"plain host rejects a low-byte percent-escape", "clickhouse://%01host/db", ""},
		// A literal (non-percent-encoded) byte that isn't valid in a host
		// (here, a space) must be rejected outright: hosts can't
		// %-encode ASCII bytes themselves, so there's no way to "fix"
		// this by escaping it after the fact.
		{"plain host rejects a literal character needing escape", "clickhouse://ho st/db", ""},
		// unescape's error message is capped at 3 characters so a long
		// garbage tail after a bad "%" doesn't get echoed back in full.
		{"long malformed escape is truncated in the error message", "clickhouse://host/%2zzzz", `"%2z"`},
		{"control character in URL", "clickhouse://host/db\n", ""},
		{"missing protocol scheme", "://host/db", ""},
		{"non-numeric port", "clickhouse://host:port/db", ""},
		{"malformed percent-encoding in path", "clickhouse://host/%zz", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			require.Error(t, err)
			if tc.errMsg != "" {
				assert.ErrorContains(t, err, tc.errMsg)
			}
		})
	}
}

func TestParseViaRequest(t *testing.T) {
	// parse's viaRequest flag is dead from Parse's perspective (always
	// called with viaRequest=false) but is retained from the net/url fork;
	// call it directly to pin down its behavior.
	cases := []struct {
		name   string
		in     string
		errMsg string
	}{
		{"empty URL", "", "empty url"},
		{"relative path without leading slash", "relative/path", "invalid URI for request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parse(tc.in, true)
			assert.ErrorContains(t, err, tc.errMsg)
		})
	}
}

func TestUnescape(t *testing.T) {
	// encodeQueryComponent is defined for API parity with net/url but is
	// never actually passed to unescape/escape from within this package —
	// RawQuery is handled by the stdlib net/url.Values parser instead.
	// Exercise it directly so the behavior is pinned down regardless.
	cases := []struct {
		name string
		in   string
		mode encoding
		want string
	}{
		{"+ becomes space in query-component mode", "a+b", encodeQueryComponent, "a b"},
		{"+ stays literal outside query-component mode", "a+b", encodePath, "a+b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := unescape(tc.in, tc.mode)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEscape(t *testing.T) {
	cases := []struct {
		name string
		in   string
		mode encoding
		want string
	}{
		{"no characters need escaping", "abc123-_.~", encodePath, "abc123-_.~"},
		// When only spaces need escaping (hexCount stays 0), escape takes
		// a separate, cheaper "+"-substitution-only code path.
		{"query-component spaces only, no hex needed", "a b c", encodeQueryComponent, "a+b+c"},
		// A "+"-for-space substitution and a "%XX" escape can both be
		// needed in the same string; both must apply correctly together.
		{"query-component space and hex escape mixed", "a b?c", encodeQueryComponent, "a+b%3Fc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, escape(tc.in, tc.mode))
		})
	}
}

func TestShouldEscape(t *testing.T) {
	// encodePathSegment and encodeUserPassword are defined for API parity
	// with net/url but churl never actually calls escape/shouldEscape with
	// them (only encodePath is used for escaping, and encodeUserPassword is
	// only ever passed to unescape, never escape). Exercise them directly.
	cases := []struct {
		name string
		c    byte
		mode encoding
		want bool
	}{
		{"path segment: slash must be escaped", '/', encodePathSegment, true},
		{"path segment: @ need not be escaped", '@', encodePathSegment, false},
		{"user/password: @ must be escaped", '@', encodeUserPassword, true},
		{"user/password: $ need not be escaped", '$', encodeUserPassword, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldEscape(tc.c, tc.mode))
		})
	}
}

func TestUnhexPanicsOnInvalidHexCharacter(t *testing.T) {
	// unhex is only ever called after ishex has validated its input, so
	// this default branch is unreachable via Parse; call it directly to
	// document that contract explicitly rather than leaving it silently
	// unverified.
	assert.Panics(t, func() {
		unhex('z')
	})
}
