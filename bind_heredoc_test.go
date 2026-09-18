package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBindHeredoc(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		want  string
	}{
		{"positional", "SELECT $$a?b$$, ?", []any{42}, "SELECT $$a?b$$, 42"},
		{"named", "SELECT $$a@b$$, @n", []any{Named("n", 42)}, "SELECT $$a@b$$, 42"},
		{"numeric", "SELECT $$1$$, $1", []any{42}, "SELECT $$1$$, 42"},
		{"mixed_false_positive", "SELECT $$a?b$$, $1", []any{42}, "SELECT $$a?b$$, 42"},
		{"tagged", "SELECT $tag_2$? $1 @n$tag_2$, ?", []any{42}, "SELECT $tag_2$? $1 @n$tag_2$, 42"},
		{"numeric_tag", "SELECT $1$? $2 @n$1$, $1", []any{42}, "SELECT $1$? $2 @n$1$, 42"},
		{"numeric_tag_positional", "SELECT $123$? $1$123$, ?", []any{42}, "SELECT $123$? $1$123$, 42"},
		{"empty_literal", "SELECT $$$$, ?", []any{42}, "SELECT $$$$, 42"},
		{"empty_tagged_literal", "SELECT $t$$t$, ?", []any{42}, "SELECT $t$$t$, 42"},
		{"raw_backslash", `SELECT $$a\?b$$, ?`, []any{42}, `SELECT $$a\?b$$, 42`},
		{"raw_delimiter", `SELECT $t$a\$t$, ?`, []any{42}, `SELECT $t$a\$t$, 42`},
		{"quotes_and_comments", "SELECT $t$'\"` -- /* ? $1 @n\n\\?$t$, ?", []any{42}, "SELECT $t$'\"` -- /* ? $1 @n\n\\?$t$, 42"},
		{"case_sensitive_tag", "SELECT $TAG$? $tag$ $1$TAG$, ?", []any{42}, "SELECT $TAG$? $tag$ $1$TAG$, 42"},
		{"different_tag", "SELECT $a$? $b$ @n $a$, ?", []any{42}, "SELECT $a$? $b$ @n $a$, 42"},
		{"several_literals", "SELECT ?, $$?$$, $t$?$t$, ?", []any{1, 2}, "SELECT 1, $$?$$, $t$?$t$, 2"},
		{"at_start", "$$? $1 @n$$ || ?", []any{"x"}, "$$? $1 @n$$ || 'x'"},
		{"only_literal", "SELECT $$? $1 @n$$", []any{42}, "SELECT $$? $1 @n$$"},
		{"identifier_boundary", "WITH 1 AS b$c$ SELECT ?, b$c$", []any{42}, "WITH 1 AS b$c$ SELECT 42, b$c$"},
		{"numeric_unchanged", "SELECT $1, $2", []any{1, 2}, "SELECT 1, 2"},
		{"single_quoted_tag", "SELECT '$t$', ?, '$t$'", []any{42}, "SELECT '$t$', 42, '$t$'"},
		{"commented_tag", "SELECT /* $t$ */ ? /* $t$ */", []any{42}, "SELECT /* $t$ */ 42 /* $t$ */"},
		{"quoted_identifier_tag", "SELECT \"$t$\", ?, \"$t$\"", []any{42}, "SELECT \"$t$\", 42, \"$t$\""},
		{"unclosed_tag", "SELECT $t$, ?", []any{42}, "SELECT $t$, 42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bind(time.UTC, tc.query, tc.args...)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
