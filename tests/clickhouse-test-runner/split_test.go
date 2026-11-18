package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStatements(t *testing.T) {
	sqls := func(stmts []statement) []string {
		out := make([]string, len(stmts))
		for i, s := range stmts {
			out[i] = s.SQL
		}
		return out
	}

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"single without semicolon", "SELECT 1", []string{"SELECT 1"}},
		{"several", "SELECT 1;\nSELECT 2;\n", []string{"SELECT 1;", "SELECT 2;"}},
		{"semicolon in string", "SELECT 'a;b'; SELECT 2", []string{"SELECT 'a;b';", "SELECT 2"}},
		{"escaped quote", `SELECT 'a\';b'; SELECT 2`, []string{`SELECT 'a\';b';`, "SELECT 2"}},
		{"doubled quote", "SELECT 'a'';b'; SELECT 2", []string{"SELECT 'a'';b';", "SELECT 2"}},
		{"semicolon in identifier", "SELECT 1 AS `a;b`; SELECT 2", []string{"SELECT 1 AS `a;b`;", "SELECT 2"}},
		{"semicolon in line comment", "SELECT 1 -- a;b\n; SELECT 2", []string{"SELECT 1 -- a;b\n;", "SELECT 2"}},
		{"semicolon in block comment", "SELECT /* a;b */ 1; SELECT 2", []string{"SELECT /* a;b */ 1;", "SELECT 2"}},
		{"nested block comment", "SELECT /* a /* b; */ c; */ 1; SELECT 2", []string{"SELECT /* a /* b; */ c; */ 1;", "SELECT 2"}},
		{"heredoc", "SELECT $$a;'b$$; SELECT $x$;$x$", []string{"SELECT $$a;'b$$;", "SELECT $x$;$x$"}},
		{"trailing comment only", "SELECT 1;\n-- the end\n", []string{"SELECT 1;"}},
		{"empty", "  \n-- only a comment\n", []string{}},
		{
			"tags header is skipped",
			"-- Tags: no-fasttest\n-- Tag no-fasttest: reason\n\nSELECT 1;",
			[]string{"SELECT 1;"},
		},
		{
			"leading comment stays with its statement",
			"SELECT 1;\n-- next\nSELECT 2;",
			[]string{"SELECT 1;", "-- next\nSELECT 2;"},
		},
		{
			"insert data ends at an empty line",
			"INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;\n\nSELECT 1;",
			[]string{"INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;", "SELECT 1;"},
		},
		{
			"insert values ends at a semicolon",
			"INSERT INTO t FORMAT Values (1);\nSELECT 1;",
			[]string{"INSERT INTO t FORMAT Values (1);", "SELECT 1;"},
		},
		{
			"format of a select is not insert data",
			"SELECT 1 FORMAT TSV;\nSELECT 2;",
			[]string{"SELECT 1 FORMAT TSV;", "SELECT 2;"},
		},
		{"double semicolon", "SELECT 1;;\nSELECT 2", []string{"SELECT 1;", "SELECT 2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sqls(splitStatements(tt.in)))
		})
	}
}

func TestSplitStatementsTrailingHint(t *testing.T) {
	stmts := splitStatements("SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2;")
	require.Len(t, stmts, 2)
	assert.Equal(t, "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }", stmts[0].Full)
	assert.Equal(t, []int32{395}, stmts[0].Hint.ServerErrors)
	assert.False(t, stmts[1].Hint.expectsError())
}

func TestSplitStatementsHintOnNextLineIsLeading(t *testing.T) {
	// clickhouse-client takes only the same-line comment as the trailing
	// comment. A hint on the next line is a leading comment of the next
	// statement, and an error hint in a leading comment is ignored.
	stmts := splitStatements("SELECT 1;\n-- { serverError 60 }\nSELECT 2;")
	require.Len(t, stmts, 2)
	assert.False(t, stmts[0].Hint.expectsError())
	assert.False(t, stmts[1].Hint.expectsError())
}

func TestSplitStatementsHintInsideStatement(t *testing.T) {
	stmts := splitStatements("SELECT x -- { serverError UNKNOWN_IDENTIFIER }\n;")
	require.Len(t, stmts, 1)
	assert.Equal(t, []int32{47}, stmts[0].Hint.ServerErrors)
}

func TestSplitStatementsEcho(t *testing.T) {
	stmts := splitStatements("SELECT 1;\n-- { echoOn }\nSELECT 2; -- two\nSELECT 3;\n-- { echoOff }\nSELECT 4;\nSELECT 5; -- { echo }\n")
	require.Len(t, stmts, 5)
	echo := make([]bool, len(stmts))
	for i, s := range stmts {
		echo[i] = s.Hint.echoThis
	}
	assert.Equal(t, []bool{false, true, true, false, true}, echo)
	assert.Equal(t, "-- { echoOn }\nSELECT 2; -- two", stmts[1].Full)
}

func TestLeadingKeyword(t *testing.T) {
	assert.Equal(t, "SELECT", leadingKeyword("  -- c\n/* d */ (select 1)"))
	assert.Equal(t, "INSERT", leadingKeyword("insert into t values (1)"))
	assert.Equal(t, "", leadingKeyword("-- only a comment"))
}

func TestSplitStatementsKeepsSemicolon(t *testing.T) {
	// clickhouse-client sends the semicolon; system.query_log shows it.
	stmts := splitStatements("SELECT 1; -- c\nSELECT 2\n")
	require.Len(t, stmts, 2)
	assert.Equal(t, "SELECT 1;", stmts[0].SQL)
	assert.Equal(t, "SELECT 2", stmts[1].SQL)
}

func TestSplitStatementsEchoOfLastStatementWithoutSemicolon(t *testing.T) {
	// clickhouse-client echoes the rest of the text, with the newlines.
	stmts := splitStatements("SELECT 1;\nSELECT 2\n\n")
	require.Len(t, stmts, 2)
	assert.Equal(t, "SELECT 1;", stmts[0].Full)
	assert.Equal(t, "SELECT 2\n\n", stmts[1].Full)
	assert.Equal(t, "SELECT 2", stmts[1].SQL)
}
