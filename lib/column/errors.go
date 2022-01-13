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

type InvalidEnum struct {
	t Type
}

func (e *InvalidEnum) Error() string {
	return fmt.Sprintf("clickhouse: invalid Enum %s", e.t)
}

type InvalidFixedSizeData struct {
	op       string
	got      int
	expected int
}

func (e *InvalidFixedSizeData) Error() string {
	return fmt.Sprintf("clickhouse [%s]: invalid fixed size data expected %d got %d", e.op, e.expected, e.got)
}

type StoreSpecialDataType struct {
	t Type
}

func (e *StoreSpecialDataType) Error() string {
	return fmt.Sprintf("clickhouse: %q data type values can't be stored in tables", e.t)
}

type BadSizeOfTuple struct {
	op       string
	got      int
	expected int
}

func (e *BadSizeOfTuple) Error() string {
	return fmt.Sprintf("clickhouse [%s]: bad size of tuple. expected %d got %d", e.op, e.expected, e.got)
}
