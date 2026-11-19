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
		{
			"insert select with a column named format",
			"INSERT INTO t SELECT format FROM s;\nSELECT 1;",
			[]string{"INSERT INTO t SELECT format FROM s;", "SELECT 1;"},
		},
		{
			"insert select from input has inline data",
			"INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2\n\nSELECT 1;",
			[]string{"INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2", "SELECT 1;"},
		},
		{
			"insert select with a format alias",
			"INSERT INTO t SELECT 1 AS format FROM numbers(1);\nSELECT 1;",
			[]string{"INSERT INTO t SELECT 1 AS format FROM numbers(1);", "SELECT 1;"},
		},
		{
			"insert select with an implicit alias after format",
			"INSERT INTO t SELECT format x FROM s;\nSELECT 1;",
			[]string{"INSERT INTO t SELECT format x FROM s;", "SELECT 1;"},
		},
		{
			"insert select with format DIV",
			"INSERT INTO t SELECT format DIV 2 FROM s;\nSELECT 1;",
			[]string{"INSERT INTO t SELECT format DIV 2 FROM s;", "SELECT 1;"},
		},
		{
			"insert select with format GLOBAL IN after FROM",
			"INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);\nSELECT 1;",
			[]string{"INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);", "SELECT 1;"},
		},
		{
			"insert from a table named format with an alias",
			"INSERT INTO dst SELECT * FROM format src;\nSELECT 2;",
			[]string{"INSERT INTO dst SELECT * FROM format src;", "SELECT 2;"},
		},
		{
			"insert from a table named format with a format-name alias",
			"INSERT INTO dst SELECT * FROM format TSV;\nSELECT 2;",
			[]string{"INSERT INTO dst SELECT * FROM format TSV;", "SELECT 2;"},
		},
		{
			"insert into a table named format, with inline data",
			"INSERT INTO format FORMAT CSV\n1\n\nSELECT 2;",
			[]string{"INSERT INTO format FORMAT CSV\n1", "SELECT 2;"},
		},
		{
			"insert into a table named format",
			"INSERT INTO format SELECT 1;\nSELECT 2;",
			[]string{"INSERT INTO format SELECT 1;", "SELECT 2;"},
		},
		{
			"insert with a comment before the format",
			"INSERT INTO t /* data: */ FORMAT CSV\n1\n\nSELECT 1;",
			[]string{"INSERT INTO t /* data: */ FORMAT CSV\n1", "SELECT 1;"},
		},
		{
			"insert with format before the data",
			"INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1\n\nSELECT 1;",
			[]string{"INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1", "SELECT 1;"},
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

func TestImplicitSelect(t *testing.T) {
	r := &runner{implicit: true}
	assert.True(t, r.implicitSelect("1 + 2"))
	// replace() is a string function, not REPLACE TABLE.
	assert.True(t, r.implicitSelect("replace('abc', 'b', 'x');"))
	assert.True(t, r.implicitSelect("-- c\nupdate ('x')"))
	assert.False(t, r.implicitSelect("REPLACE TABLE t AS SELECT 1"))
	assert.False(t, r.implicitSelect("DELETE FROM t WHERE x = 1"))
	assert.False(t, r.implicitSelect("INSERT INTO t VALUES (1)"))
	assert.False(t, (&runner{}).implicitSelect("1 + 2"))
}

func TestReturnsRows(t *testing.T) {
	for _, q := range []string{"SELECT 1", "with x AS (SELECT 1) SELECT * FROM x", "FROM t SELECT x", "VALUES (1), (2)", "TABLE t", "-- c\n(SELECT 1)"} {
		assert.True(t, returnsRows(q), q)
	}
	for _, q := range []string{"INSERT INTO t VALUES (1)", "CREATE TABLE t (x UInt8) ENGINE = Memory", "SET x = 1", "DROP TABLE t"} {
		assert.False(t, returnsRows(q), q)
	}
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

func TestTopLevelWords(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			"literals, heredocs, comments and parentheses are skipped",
			"SELECT a, 'b c', `d`, \"e\", $$f g$$, $t$h$t$ -- i\nFROM (SELECT j) /* k */",
			[]string{"SELECT", "a", "FROM"},
		},
		{
			"FROM in a heredoc",
			"INSERT INTO t SELECT $$x FROM y$$ FROM (SELECT z)",
			[]string{"INSERT", "INTO", "t", "SELECT", "FROM"},
		},
		{
			"a dollar sign in an identifier does not start a heredoc",
			"SELECT 1 AS $a$b$ FROM t",
			[]string{"SELECT", "1", "AS", "a", "b", "FROM", "t"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, topLevelWords(tt.in))
		})
	}
}

func TestSignificantTokens(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			"quotes and comments",
			"SELECT 'a', `b` /* c */ -- d\n+ \"e\"",
			[]string{"SELECT", "'", ",", "`", "+", `"`},
		},
		{
			"a heredoc is one string literal",
			"SELECT $$a -- b$$ + $t$ 'c /* $t$, x",
			[]string{"SELECT", "'", "+", "'", ",", "x"},
		},
		{
			"a dollar sign in an identifier does not start a heredoc",
			"SELECT $a$b$ * x",
			[]string{"SELECT", "$", "a", "$", "b", "$", "*", "x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, significantTokens(tt.in))
		})
	}
}
