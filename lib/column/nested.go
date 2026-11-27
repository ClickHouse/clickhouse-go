package column

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

type Nested struct {
	Interface
	name string
}

func (col *Nested) Reset() {
	col.Interface.Reset()
}

func asDDL(cols []namedCol) string {
	sCols := make([]string, len(cols))
	for i := range cols {
		sCols[i] = fmt.Sprintf("%s %s", cols[i].name, cols[i].colType)
	}
	return strings.Join(sCols, ", ")
}

func (col *Nested) parse(t Type, sc *ServerContext) (_ Interface, err error) {
	columns := fmt.Sprintf("Array(Tuple(%s))", asDDL(nestedColumns(t.params())))
	if col.Interface, err = (&Array{name: col.name}).parse(Type(columns), sc); err != nil {
		return nil, err
	}
	return col, nil
}

func nestedColumns(raw string) (columns []namedCol) {
	for {
		i := indexTopLevelComma(raw)
		if i < 0 {
			i = len(raw)
		}
		element := strings.TrimSpace(raw[:i])
		_, colType := splitTupleElement(element)
		columns = append(columns, namedCol{
			// Keep the quoted name for the Tuple parser that consumes this DDL.
			name:    strings.TrimSpace(element[:len(element)-len(colType)]),
			colType: Type(colType),
		})
		if i == len(raw) {
			break
		}
		raw = raw[i+1:]
	}
	for i, column := range columns {
		if strings.HasPrefix(string(column.colType), "Nested(") {
			columns[i] = namedCol{
				colType: Type(fmt.Sprintf("Array(Tuple(%s))", asDDL(nestedColumns(column.colType.params())))),
				name:    column.name,
			}
		}
	}
	return
}

func (col *Nested) ReadStatePrefix(reader *proto.Reader) error {
	if serialize, ok := col.Interface.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(reader); err != nil {
			return err
		}
	}
	return nil
}

func (col *Nested) WriteStatePrefix(buffer *proto.Buffer) error {
	if serialize, ok := col.Interface.(CustomSerialization); ok {
		if err := serialize.WriteStatePrefix(buffer); err != nil {
			return err
		}
	}
	return nil
}

var _ Interface = (*Nested)(nil)
