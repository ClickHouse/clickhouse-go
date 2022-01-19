package proto

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

type BlockError struct {
	Op         string
	Err        error
	ColumnName string
}

func (e *BlockError) Error() string {
	switch err := e.Err.(type) {
	case *column.Error:
		return fmt.Sprintf("clickhouse [%s]: (%s %s) %s", e.Op, e.ColumnName, err.ColumnType, err.Err)
	case *column.ColumnConverterError:
		var hint string
		if len(err.Hint) != 0 {
			hint += ". " + err.Hint
		}
		return fmt.Sprintf("clickhouse [%s]: (%s) converting %s to %s is unsupported%s",
			err.Op, e.ColumnName,
			err.From, err.To,
			hint,
		)
	}
	return fmt.Sprintf("clickhouse [%s]: %s %s", e.Op, e.ColumnName, e.Err)
}
