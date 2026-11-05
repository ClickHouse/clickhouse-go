package clickhouse

import (
	"testing"
)

// Both parsers consume whatever a server or proxy hands us. The invariants
// under arbitrary input: never panic, and never produce a typed Exception
// with a non-positive code.

func FuzzParseHTTPException(f *testing.F) {
	f.Add("Code: 60. DB::Exception: Unknown table 'foo'. (UNKNOWN_TABLE) (version 25.1.5.31 (official build))", "", "")
	f.Add("Code: 62. DB::Exception: Syntax error:\nmultiline. (SYNTAX_ERROR)", "62", "SYNTAX_ERROR")
	f.Add("<html><body>502 Bad Gateway</body></html>", "", "")
	f.Add("", "60", "UNKNOWN_TABLE")
	f.Add("Code: 0. DB::Exception: zero", "0", "not-a-symbol")
	f.Add("Code: 999999999999999999999. DB::Exception: overflow", "-1", "")
	f.Add("Code: 210. DB::NetException: Connection refused", "abc", "a b")
	f.Add("Code: 60.", "", "X")

	f.Fuzz(func(t *testing.T, text, headerCode, headerName string) {
		ex := parseHTTPException(text, headerCode, headerName)
		if ex == nil {
			return
		}
		if ex.Code <= 0 {
			t.Errorf("parsed exception with non-positive code %d from text=%q header=%q", ex.Code, text, headerCode)
		}
		if ex.CodeName != "" && !exceptionCodeNameValueRe.MatchString(ex.CodeName) {
			t.Errorf("parsed exception with malformed CodeName %q from text=%q headerName=%q", ex.CodeName, text, headerName)
		}
		if ex.Error() == "" {
			t.Errorf("parsed exception with empty Error() from text=%q header=%q", text, headerCode)
		}
	})
}

func FuzzParseExceptionFromBytes(f *testing.F) {
	f.Add([]byte("\r\n__exception__\r\n1234567890123456\r\nCode: 395. DB::Exception: boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n42 1234567890123456\r\n__exception__\r\n"))
	f.Add([]byte("\r\n__exception__\r\n1234567890123456\r\nDB::Exception: no code prefix"))
	f.Add([]byte("__exception__"))
	f.Add([]byte("__exception__\r\n\r\n\r\n__exception__"))
	f.Add([]byte("no marker at all"))
	f.Add([]byte{})
	f.Add([]byte("\r\n__exception__\r\n\x00\x01\x02binary\xff\r\n__exception__\r\n"))
	f.Add([]byte("__exception__\r\nCode: 395. DB::Exception: fake Code: 60. DB::Exception: injected. (X)\n"))
	f.Add([]byte("__exception__\r\nCode: 395. DB::Exception: truncat\nexception__\nCode: 395. DB::Exception: full. (Y)\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		err := parseExceptionFromBytes(data)
		if err == nil {
			t.Errorf("parseExceptionFromBytes returned nil error for %q", data)
			return
		}
		if err.Error() == "" {
			t.Errorf("empty error message for %q", data)
		}
	})
}
