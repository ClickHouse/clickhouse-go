package parser_utils

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/parser_utils/lexer"
	"github.com/antlr4-go/antlr/v4"
	"strings"
)

func ExtractQueryAndColumns(query string) (string, []string, error) {
	queryBuilder := strings.Builder{}
	columns := make([]string, 0, 128)
	inputStream := antlr.NewInputStream(query)
	currLexer := lexer.NewClickHouseLexer(inputStream)
	lParenPassed := false
	valuesPresent := false
	for {
		t := currLexer.NextToken()
		if t.GetTokenType() == antlr.TokenEOF {
			break
		}
		queryBuilder.WriteString(t.GetText())
		if t.GetTokenType() == lexer.ClickHouseLexerVALUES && strings.ToUpper(t.GetText()) == "VALUES" {
			valuesPresent = true
			break
		}
		queryBuilder.WriteString(" ")
		if lParenPassed && t.GetTokenType() != lexer.ClickHouseLexerCOMMA && t.GetTokenType() != lexer.ClickHouseLexerRPAREN {
			columnName := strings.Trim(strings.Trim(strings.TrimSpace(t.GetText()), "\""), "`")
			columns = append(columns, columnName)
		}
		if t.GetTokenType() == lexer.ClickHouseLexerLPAREN {
			lParenPassed = true
		}
	}

	if !valuesPresent {
		queryBuilder.WriteString("VALUES")
	}
	return queryBuilder.String(), columns, nil
}
