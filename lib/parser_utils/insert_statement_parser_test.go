package parser_utils

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func check(t *testing.T, stmt string, outQueryExpected string, outColumnsExpected []string) {
	query, columns, err := ExtractQueryAndColumns(stmt)
	require.NoError(t, err)
	require.Equal(t, outQueryExpected, query)
	require.Equal(t, outColumnsExpected, columns)
}

func TestExtractQueryAndColumns(t *testing.T) {
	// column name with parentheses
	check(
		t,
		"INSERT INTO `my_schema`.`my_table1` (`__primary_key`,`ColumnNameWithParentheses(something)`) VALUES (1,1);",
		"INSERT INTO `my_schema` . `my_table1` ( `__primary_key` , `ColumnNameWithParentheses(something)` ) VALUES",
		[]string{"__primary_key", "ColumnNameWithParentheses(something)"},
	)

	// 'VALUE' is absent
	check(
		t,
		"INSERT INTO my_table2",
		"INSERT INTO my_table2 VALUES",
		[]string{},
	)

	// column_name (id) is matched with not-identifier token
	check(
		t,
		"INSERT INTO my_table3 (id, ts)",
		"INSERT INTO my_table3 ( id , ts ) VALUES",
		[]string{"id", "ts"},
	)

	// column_name in quotes
	check(
		t,
		`INSERT INTO my_table4 ("id")`,
		`INSERT INTO my_table4 ( "id" ) VALUES`,
		[]string{"id"},
	)
}
