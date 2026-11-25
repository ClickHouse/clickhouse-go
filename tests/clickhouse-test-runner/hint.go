package main

import (
	"fmt"
	"strconv"
	"strings"
)

// testHint holds the test hints of one statement, as `clickhouse-client`
// parses them (see `src/Client/TestHint.cpp`):
//
//	SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
//	SELECT 1 / 0;      -- { serverError 153, 6 }
//	-- { echoOn }
//
// An error hint counts only when it is not in a leading comment. An `echo`
// hint counts in any comment.
type testHint struct {
	ServerErrors []int32
	ClientErrors []int32
	// Echo is set by `echo`, `echoOn` (true) or `echoOff` (false).
	Echo *bool
	// ParseError is set when a hint is malformed or names an unknown error.
	ParseError error

	// echoThis tells if this statement is printed before it runs.
	echoThis bool
}

func (h testHint) expectsError() bool {
	return len(h.ServerErrors) > 0 || len(h.ClientErrors) > 0
}

// matches tells if the error code satisfies the hint. Over clickhouse-go the
// server finds every error, including syntax errors that `clickhouse-client`
// finds locally, so a server error can satisfy a `clientError` hint too.
func (h testHint) matches(code int32) bool {
	for _, c := range h.ServerErrors {
		if c == code {
			return true
		}
	}
	for _, c := range h.ClientErrors {
		if c == code {
			return true
		}
	}
	return false
}

func (h testHint) label() string {
	codes := func(list []int32) string {
		parts := make([]string, len(list))
		for i, c := range list {
			parts[i] = strconv.Itoa(int(c))
			if name, ok := errorNames[c]; ok {
				parts[i] = name
			}
		}
		return strings.Join(parts, ", ")
	}
	switch {
	case len(h.ServerErrors) > 0 && len(h.ClientErrors) > 0:
		return "error " + codes(h.ServerErrors)
	case len(h.ServerErrors) > 0:
		return "serverError " + codes(h.ServerErrors)
	default:
		return "clientError " + codes(h.ClientErrors)
	}
}

func parseTestHint(full string) testHint {
	var h testHint
	leading := true
	for i := 0; i < len(full); {
		var end int
		switch {
		case isSpace(full[i]):
			i++
			continue
		case strings.HasPrefix(full[i:], "--"),
			strings.HasPrefix(full[i:], "#!"), strings.HasPrefix(full[i:], "# "):
			end = skipLineComment(full, i)
		case strings.HasPrefix(full[i:], "/*"):
			end = skipBlockComment(full, i)
		case full[i] == '\'' || full[i] == '"' || full[i] == '`':
			leading = false
			i = skipQuoted(full, i)
			continue
		case full[i] == '$':
			leading = false
			if e := heredocEnd(full, i); e > 0 {
				i = e
			} else {
				i++
			}
			continue
		default:
			leading = false
			i++
			continue
		}
		comment := full[i:end]
		if open := strings.IndexByte(comment, '{'); open >= 0 {
			if closing := strings.IndexByte(comment[open:], '}'); closing >= 0 {
				h.parse(comment[open+1:open+closing], leading)
			}
		}
		i = end
	}
	return h
}

func (h *testHint) parse(body string, leading bool) {
	tokens := tokenizeHint(body)
	for i := 0; i < len(tokens); i++ {
		switch tok := tokens[i]; tok {
		case "echo", "echoOn":
			on := true
			h.Echo = &on
		case "echoOff":
			off := false
			h.Echo = &off
		case "retry":
			// `retry N until|while serverError X` is used by a few tests
			// that wait for a background process. It is not supported.
			h.ParseError = fmt.Errorf("the %q test hint is not supported", "retry")
			return
		case "serverError", "clientError", "error":
			if leading {
				continue
			}
			var codes []int32
			for _, t := range tokens[i+1:] {
				if t == "," {
					continue
				}
				if n, err := strconv.ParseInt(t, 10, 32); err == nil {
					codes = append(codes, int32(n))
					continue
				}
				code, ok := errorCodes[t]
				if !ok {
					h.ParseError = fmt.Errorf("unknown error name %q in test hint", t)
					return
				}
				codes = append(codes, code)
			}
			switch tok {
			case "serverError":
				h.ServerErrors = codes
			case "clientError":
				h.ClientErrors = codes
			default:
				h.ServerErrors, h.ClientErrors = codes, codes
			}
			return
		}
	}
}

func tokenizeHint(body string) []string {
	var out []string
	for i := 0; i < len(body); {
		switch c := body[i]; {
		case isSpace(c):
			i++
		case c == ',':
			out = append(out, ",")
			i++
		default:
			j := i
			for j < len(body) && !isSpace(body[j]) && body[j] != ',' {
				j++
			}
			out = append(out, body[i:j])
			i = j
		}
	}
	return out
}
