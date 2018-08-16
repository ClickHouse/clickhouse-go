package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type wmTest struct {
	haystack string
	needle   string
	expect   bool
}

func checkMatch(haystack, needle string) bool {
	m := newMatcher(needle)
	for _, r := range []rune(haystack) {
		if m.matchRune(r) {
			return true
		}
	}
	return false
}

func TestWordMatcher(t *testing.T) {

	table := []wmTest{
		wmTest{"select * from test", "select", true},
		wmTest{"select * from test", "*", true},
		wmTest{"select * from test", "elect", true},
		wmTest{"select * from test", "zelect", false},
		wmTest{"select * from test", "sElEct", true},
	}

	for _, test := range table {
		assert.Equal(t, checkMatch(test.haystack, test.needle), test.expect, test.haystack)
	}

}
