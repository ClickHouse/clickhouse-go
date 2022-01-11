package column

import "fmt"

type ColumnConverterErr struct {
	op   string
	to   string
	from string
}

func (e *ColumnConverterErr) Error() string {
	return fmt.Sprintf("clickhouse: %s: converting %s to %s is unsupported", e.op, e.from, e.to)
}

type UnknownElementForEnum struct {
	element string
}

func (e *UnknownElementForEnum) Error() string {
	return fmt.Sprintf("clickhouse: unknown element %q for enum", e.element)
}
