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

func TestParseBasicDSN(t *testing.T) {
	u, err := Parse("clickhouse://user:pass@localhost:9000/default?dial_timeout=1s")
	require.NoError(t, err)
	assert.Equal(t, "clickhouse", u.Scheme)
	assert.Equal(t, "localhost:9000", u.Host)
	assert.Equal(t, "/default", u.Path)
	assert.Equal(t, "dial_timeout=1s", u.RawQuery)
	require.NotNil(t, u.User)
	assert.Equal(t, "user", u.User.Username())
	pass, ok := u.User.Password()
	assert.True(t, ok)
	assert.Equal(t, "pass", pass)
}

func TestParseMultiHostDSN(t *testing.T) {
	// This is the entire reason churl exists: net/url.Parse (>= Go 1.26)
	// rejects commas in the host part. clickhouse-go relies on getting the
	// raw comma-separated host string back intact so it can split it into
	// individual addresses itself (see Options.fromDSN).
	u, err := Parse("clickhouse://host1:9000,host2:9001,host3:9000/db")
	require.NoError(t, err)
	assert.Equal(t, "host1:9000,host2:9001,host3:9000", u.Host)
	assert.Equal(t, "/db", u.Path)
}

func TestParseMultiHostDSNWithoutPorts(t *testing.T) {
	u, err := Parse("clickhouse://host1,host2,host3/db")
	require.NoError(t, err)
	assert.Equal(t, "host1,host2,host3", u.Host)
}

func TestParseMultiHostDSNWithAuthAndQuery(t *testing.T) {
	u, err := Parse("clickhouse://user:pass@host1:9000,host2:9000/default?secure=true")
	require.NoError(t, err)
	assert.Equal(t, "host1:9000,host2:9000", u.Host)
	assert.Equal(t, "user", u.User.Username())
	assert.Equal(t, "secure=true", u.RawQuery)
}

func TestParseIPv6Host(t *testing.T) {
	u, err := Parse("clickhouse://[::1]:9000/db")
	require.NoError(t, err)
	assert.Equal(t, "[::1]:9000", u.Host)
}

func TestParseIPv6HostRejectsIPv4Literal(t *testing.T) {
	// Per RFC 3986, only IPv6 addresses may be bracketed.
	_, err := Parse("clickhouse://[127.0.0.1]:9000/db")
	assert.Error(t, err)
}

func TestParseNoScheme(t *testing.T) {
	u, err := Parse("localhost:9000/db")
	require.NoError(t, err)
	// A bare "host:port" with no "//" is parsed as an opaque scheme:path,
	// mirroring net/url's own behavior for e.g. "mailto:" style URLs. This
	// documents that DSNs handed to fromDSN must include the "clickhouse://"
	// (or "http(s)://") prefix or they will not parse as a Host at all.
	assert.Equal(t, "localhost", u.Scheme)
	assert.Equal(t, "9000/db", u.Opaque)
	assert.Empty(t, u.Host)
}

func TestParseAtSignInPassword(t *testing.T) {
	// Documented quirk in parseAuthority/validUserinfo: '@' is technically
	// a delimiter between userinfo and host, but is tolerated inside
	// userinfo for compatibility (see https://go.dev/issue/3439). The
	// rightmost '@' is treated as the real delimiter.
	u, err := Parse("http://username:p@ssword@example.com/db")
	require.NoError(t, err)
	assert.Equal(t, "example.com", u.Host)
	assert.Equal(t, "username", u.User.Username())
	pass, ok := u.User.Password()
	assert.True(t, ok)
	assert.Equal(t, "p@ssword", pass)
}

func TestParseQueryParametersWithHAHosts(t *testing.T) {
	u, err := Parse("clickhouse://host1:9440,host2:9440/default?secure=true&skip_verify=true")
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "true", q.Get("secure"))
	assert.Equal(t, "true", q.Get("skip_verify"))
}

func TestParseForceQuery(t *testing.T) {
	u, err := Parse("clickhouse://host/db?")
	require.NoError(t, err)
	assert.True(t, u.ForceQuery)
	assert.Empty(t, u.RawQuery)
}

func TestParseFragmentIsStripped(t *testing.T) {
	u, err := Parse("clickhouse://host/db?x=1#section")
	require.NoError(t, err)
	assert.Equal(t, "section", u.Fragment)
	assert.Equal(t, "x=1", u.RawQuery)
}

func TestParseEscapedPath(t *testing.T) {
	u, err := Parse("clickhouse://host/my%20db")
	require.NoError(t, err)
	assert.Equal(t, "/my db", u.Path)
	assert.Equal(t, "/my%20db", u.EscapedPath())
}

func TestParseRejectsControlCharacters(t *testing.T) {
	_, err := Parse("clickhouse://host/db\n")
	assert.Error(t, err)
}

func TestParseRejectsMissingProtocolScheme(t *testing.T) {
	_, err := Parse("://host/db")
	assert.Error(t, err)
}

func TestParseRejectsInvalidPort(t *testing.T) {
	_, err := Parse("clickhouse://host:port/db")
	assert.Error(t, err)
}

func TestParseRejectsBadPercentEncoding(t *testing.T) {
	_, err := Parse("clickhouse://host/%zz")
	assert.Error(t, err)
}

func TestParseEmptyHostWithScheme(t *testing.T) {
	u, err := Parse("clickhouse:///db")
	require.NoError(t, err)
	assert.Empty(t, u.Host)
	assert.Equal(t, "/db", u.Path)
}

func TestParseOmitHostSingleSlash(t *testing.T) {
	// A single slash after the scheme (no "//" authority marker at all)
	// sets OmitHost, distinct from an explicit-but-empty "///" authority.
	u, err := Parse("clickhouse:/db")
	require.NoError(t, err)
	assert.Empty(t, u.Host)
	assert.True(t, u.OmitHost)
	assert.Equal(t, "/db", u.Path)
}

func TestParseWildcard(t *testing.T) {
	u, err := Parse("*")
	require.NoError(t, err)
	assert.Equal(t, "*", u.Path)
}

func TestParseFragmentUnescapeError(t *testing.T) {
	_, err := Parse("clickhouse://host/db#%zz")
	assert.Error(t, err)
}

func TestParseFirstPathSegmentContainsColonError(t *testing.T) {
	// With no scheme and no leading "/", a colon in the first path segment
	// is ambiguous with a scheme separator (RFC 3986 §3.3) and must be
	// rejected rather than silently treated as part of the path.
	_, err := Parse("0:foo/bar")
	assert.ErrorContains(t, err, "first path segment in URL cannot contain colon")
}

func TestParseInvalidLeadingSchemeCharacter(t *testing.T) {
	// getScheme bails out of scheme detection as soon as it sees a
	// character that can't start or continue one; the whole string is then
	// treated as a relative path instead of erroring.
	u, err := Parse("@foo/bar")
	require.NoError(t, err)
	assert.Empty(t, u.Scheme)
	assert.Equal(t, "@foo/bar", u.Path)
}

func TestParseRelativePathNoScheme(t *testing.T) {
	// A string made entirely of scheme-legal letters but with no ':'
	// anywhere falls through getScheme's loop with no scheme found.
	u, err := Parse("abcdef")
	require.NoError(t, err)
	assert.Empty(t, u.Scheme)
	assert.Equal(t, "abcdef", u.Path)
}

func TestParseInvalidUserinfoCharacter(t *testing.T) {
	_, err := Parse("clickhouse://user name@host/db")
	assert.ErrorContains(t, err, "invalid userinfo")
}

func TestParseUserinfoWithUppercaseAndDigits(t *testing.T) {
	// validUserinfo has separate branches for A-Z, a-z, and 0-9; exercise
	// all three (lowercase already covered by other tests in this file).
	u, err := Parse("clickhouse://User123:Pass456@host/db")
	require.NoError(t, err)
	assert.Equal(t, "User123", u.User.Username())
	pass, ok := u.User.Password()
	assert.True(t, ok)
	assert.Equal(t, "Pass456", pass)
}

func TestParseUserinfoWithoutPassword(t *testing.T) {
	u, err := Parse("clickhouse://justuser@host/db")
	require.NoError(t, err)
	assert.Equal(t, "justuser", u.User.Username())
	_, ok := u.User.Password()
	assert.False(t, ok)
}

func TestParseUsernameUnescapeError(t *testing.T) {
	_, err := Parse("clickhouse://%zz:pass@host/db")
	assert.Error(t, err)
}

func TestParsePasswordUnescapeError(t *testing.T) {
	_, err := Parse("clickhouse://user:%zz@host/db")
	assert.Error(t, err)
}

func TestParseIPv6MissingCloseBracket(t *testing.T) {
	_, err := Parse("clickhouse://[::1/db")
	assert.ErrorContains(t, err, "missing ']' in host")
}

func TestParseIPv6InvalidPort(t *testing.T) {
	_, err := Parse("clickhouse://[::1]:abc/db")
	assert.ErrorContains(t, err, "invalid port")
}

func TestParseIPv6ZoneIdentifier(t *testing.T) {
	// RFC 6874: %25 introduces a zone identifier for a link-local address,
	// e.g. "fe80::1%en0". The zone itself may use its own %-escaping rules.
	u, err := Parse("clickhouse://[fe80::1%25en0]:9000/db")
	require.NoError(t, err)
	assert.Equal(t, "[fe80::1%en0]:9000", u.Host)
}

func TestParseIPv6HostnameUnescapeError(t *testing.T) {
	// No "%25" zone marker present, so the whole bracketed hostname goes
	// through the plain (non-zone) unescape path, which must still reject
	// malformed escapes.
	_, err := Parse("clickhouse://[fe80::1%zz]:9000/db")
	assert.Error(t, err)
}

func TestParseIPv6ZoneRejectsDisallowedEscape(t *testing.T) {
	// Inside a zone identifier, only "%25" itself and escapes that decode
	// to characters already valid unescaped in a host are allowed. "%2F"
	// decodes to '/', which must stay escaped, so this is rejected.
	_, err := Parse("clickhouse://[fe80::1%25%2F]:9000/db")
	assert.Error(t, err)
}

func TestParseIPv6InvalidAddress(t *testing.T) {
	_, err := Parse("clickhouse://[not-an-ip]:9000/db")
	assert.ErrorContains(t, err, "invalid host")
}

func TestParseIPv6NoPort(t *testing.T) {
	u, err := Parse("clickhouse://[::1]/db")
	require.NoError(t, err)
	assert.Equal(t, "[::1]", u.Host)
}

func TestParseIPv6TrailingGarbageAfterBracket(t *testing.T) {
	// Anything after the closing "]" that isn't a valid ":port" is rejected.
	_, err := Parse("clickhouse://[::1]xyz/db")
	assert.ErrorContains(t, err, "invalid port")
}

func TestParseHostUnescapeError(t *testing.T) {
	// Plain (non-bracketed) host, no port: goes through parseHost's final
	// unescape call.
	_, err := Parse("clickhouse://%zz/db")
	assert.Error(t, err)
}

func TestParseHostRejectsLowByteEscape(t *testing.T) {
	// Per RFC 3986 §3.2.2, %-encoding in a plain host component is only
	// meaningful for non-ASCII bytes; encoding an ASCII byte below 0x80
	// (other than via the reserved "%25") is rejected even though "%01" is
	// syntactically well-formed hex.
	_, err := Parse("clickhouse://%01host/db")
	assert.Error(t, err)
}

func TestParseTruncatesLongMalformedEscapeInErrorMessage(t *testing.T) {
	// unescape's error message is capped at 3 characters so a long garbage
	// tail after a bad "%" doesn't get echoed back in full.
	_, err := Parse("clickhouse://host/%2zzzz")
	assert.ErrorContains(t, err, `"%2z"`)
}

func TestParseFragmentSetsRawFragmentWhenEscapingDiffers(t *testing.T) {
	// setFragment only populates RawFragment when the default re-escaping
	// of the decoded fragment would not reproduce the original text
	// byte-for-byte (here, a literal space needs escaping to round-trip).
	u, err := Parse("clickhouse://host/db#a b")
	require.NoError(t, err)
	assert.Equal(t, "a b", u.Fragment)
	assert.Equal(t, "a b", u.RawFragment)
}

func TestParsePathSetsRawPathWhenEscapingDiffers(t *testing.T) {
	// "%2F" decodes to '/', but re-escaping a decoded '/' for encodePath
	// does not re-add the percent-encoding (a literal '/' is valid in a
	// path), so the default escaping doesn't reproduce the original text
	// and RawPath must be kept to preserve it.
	u, err := Parse("clickhouse://host/foo%2Fbar")
	require.NoError(t, err)
	assert.Equal(t, "/foo/bar", u.Path)
	assert.Equal(t, "/foo%2Fbar", u.RawPath)
}

func TestParseViaRequestEmptyURL(t *testing.T) {
	// parse's viaRequest flag is dead from Parse's perspective (always
	// called with viaRequest=false) but is retained from the net/url fork;
	// call it directly to pin down its behavior.
	_, err := parse("", true)
	assert.ErrorContains(t, err, "empty url")
}

func TestParseViaRequestRelativePath(t *testing.T) {
	_, err := parse("relative/path", true)
	assert.ErrorContains(t, err, "invalid URI for request")
}

func TestUnescapeQueryComponentPlusBecomesSpace(t *testing.T) {
	// encodeQueryComponent is defined for API parity with net/url but is
	// never actually passed to unescape/escape from within this package —
	// RawQuery is handled by the stdlib net/url.Values parser instead.
	// Exercise it directly so the behavior is pinned down regardless.
	got, err := unescape("a+b", encodeQueryComponent)
	require.NoError(t, err)
	assert.Equal(t, "a b", got)
}

func TestUnescapePlusIsLiteralOutsideQueryComponent(t *testing.T) {
	got, err := unescape("a+b", encodePath)
	require.NoError(t, err)
	assert.Equal(t, "a+b", got)
}

func TestEscapeQueryComponentSpaceAndHexMixed(t *testing.T) {
	// When a query-component string needs both a "+"-for-space substitution
	// and a "%XX" escape in the same string, both must apply correctly.
	got := escape("a b?c", encodeQueryComponent)
	assert.Equal(t, "a+b%3Fc", got)
}

func TestEscapeNoCharactersNeedEscaping(t *testing.T) {
	got := escape("abc123-_.~", encodePath)
	assert.Equal(t, "abc123-_.~", got)
}

func TestParseUsernameOnlyUnescapeError(t *testing.T) {
	// No ':' in the userinfo (username only, no password): a distinct
	// unescape call site from the username+password case.
	_, err := Parse("clickhouse://%zz@host/db")
	assert.Error(t, err)
}

func TestParseIPv6ZoneHostPartUnescapeError(t *testing.T) {
	// The portion of the bracketed hostname *before* the "%25" zone marker
	// is unescaped separately from the zone itself; a bad escape there must
	// also be rejected.
	_, err := Parse("clickhouse://[%zz%25en0]:9000/db")
	assert.Error(t, err)
}

func TestParseHostLiteralCharacterNeedingEscape(t *testing.T) {
	// A literal (non-percent-encoded) byte that isn't valid in a host
	// (here, a space) must be rejected outright rather than silently
	// accepted — hosts can't %-encode ASCII bytes themselves, so there's no
	// way to "fix" this by escaping it after the fact.
	_, err := Parse("clickhouse://ho st/db")
	assert.Error(t, err)
}

func TestParseUsernameLiteralPlusSurvivesAlongsidePercentEscape(t *testing.T) {
	// '+' is only special (decodes to space) in encodeQueryComponent mode;
	// in userinfo it stays literal even when the same string also contains
	// a "%XX" escape that forces the unescape rebuild path to run.
	u, err := Parse("clickhouse://a+b%20c:pass@host/db")
	require.NoError(t, err)
	assert.Equal(t, "a+b c", u.User.Username())
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

func TestEscapeQueryComponentSpacesOnlyNoHexNeeded(t *testing.T) {
	// Distinct from TestEscapeQueryComponentSpaceAndHexMixed: when *only*
	// spaces need escaping (hexCount stays 0), escape takes a separate,
	// cheaper "+"-substitution-only code path.
	got := escape("a b c", encodeQueryComponent)
	assert.Equal(t, "a+b+c", got)
}

func TestParseFragmentReservedCharactersStayUnescaped(t *testing.T) {
	// RFC 3986 §4.1 allows the full reserved set unescaped in a fragment;
	// churl's shouldEscape mirrors that for the encodeFragment mode.
	u, err := Parse("clickhouse://host/db#a=b&c")
	require.NoError(t, err)
	assert.Equal(t, "a=b&c", u.Fragment)
	assert.Empty(t, u.RawFragment)
}

func TestParseFragmentSubDelimsStayUnescaped(t *testing.T) {
	u, err := Parse("clickhouse://host/db#a(b)*c!d")
	require.NoError(t, err)
	assert.Equal(t, "a(b)*c!d", u.Fragment)
	assert.Empty(t, u.RawFragment)
}

func TestParseLowercaseHexEscape(t *testing.T) {
	// ishex/unhex each branch separately on lowercase vs uppercase hex
	// digits; exercise the lowercase side ('e' in "%7e" decodes to '~').
	u, err := Parse("clickhouse://host/%7e")
	require.NoError(t, err)
	assert.Equal(t, "/~", u.Path)
}

func TestShouldEscapePathSegmentReservedCharacters(t *testing.T) {
	// encodePathSegment is defined for API parity with net/url but churl
	// never actually calls escape/shouldEscape with it (only encodePath is
	// used, for the path as a whole). Exercise it directly.
	assert.True(t, shouldEscape('/', encodePathSegment))
	assert.False(t, shouldEscape('@', encodePathSegment))
}

func TestShouldEscapeUserPasswordReservedCharacters(t *testing.T) {
	// Similarly, encodeUserPassword is only ever passed to unescape (for
	// userinfo), never to escape/shouldEscape, since churl never
	// re-encodes a Userinfo back into a URL string. Exercise it directly.
	assert.True(t, shouldEscape('@', encodeUserPassword))
	assert.False(t, shouldEscape('$', encodeUserPassword))
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
