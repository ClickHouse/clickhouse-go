package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTestHint(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		server []int32
		client []int32
	}{
		{"server error by name", "SELECT 1; -- { serverError UNKNOWN_TABLE }", []int32{60}, nil},
		{"server error by code", "SELECT 1; -- { serverError 60 }", []int32{60}, nil},
		{"several codes", "SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }", []int32{153, 153}, nil},
		{"client error", "SELECT; -- { clientError SYNTAX_ERROR }", nil, []int32{62}},
		{"error sets both", "SELECT 1; -- { error 62 }", []int32{62}, []int32{62}},
		{"block comment", "SELECT 1 /* { serverError 60 } */;", []int32{60}, nil},
		{"text after the hint", "SELECT 1; -- { serverError 60 } because there is no table", []int32{60}, nil},
		{"leading hint is ignored", "-- { serverError 60 }\nSELECT 1;", nil, nil},
		{"braces in a string are not a hint", "SELECT '-- { serverError 60 }';", nil, nil},
		{"no hint", "SELECT 1; -- a comment", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := parseTestHint(tt.in)
			require.NoError(t, h.ParseError)
			assert.Equal(t, tt.server, h.ServerErrors)
			assert.Equal(t, tt.client, h.ClientErrors)
		})
	}
}

func TestParseTestHintErrors(t *testing.T) {
	assert.Error(t, parseTestHint("SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }").ParseError)
	assert.Error(t, parseTestHint("SELECT 1; -- { retry 3 until serverError 60 }").ParseError)
}

func TestTestHintMatches(t *testing.T) {
	h := parseTestHint("SELECT; -- { clientError SYNTAX_ERROR }")
	// Over clickhouse-go the server finds syntax errors.
	assert.True(t, h.matches(62))
	assert.False(t, h.matches(60))
	assert.Equal(t, "clientError SYNTAX_ERROR", h.label())
}

func TestParseTestHintEcho(t *testing.T) {
	h := parseTestHint("-- { echoOn }\nSELECT 1;")
	require.NotNil(t, h.Echo)
	assert.True(t, *h.Echo)
	h = parseTestHint("SELECT 1; -- { echoOff }")
	require.NotNil(t, h.Echo)
	assert.False(t, *h.Echo)
}
