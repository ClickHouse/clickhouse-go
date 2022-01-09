package external

import (
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

func NewTable(name string, columns ...func(t *Table) error) (*Table, error) {
	table := &Table{
		name:  name,
		block: &proto.Block{},
	}
	for _, column := range columns {
		if err := column(table); err != nil {
			return nil, err
		}
	}
	return table, nil
}

type Table struct {
	name  string
	block *proto.Block
}

func (tbl *Table) Name() string {
	return tbl.name
}

func (tbl *Table) Block() *proto.Block {
	return tbl.block
}

func (tbl *Table) Append(v ...interface{}) error {
	return tbl.block.Append(v...)
}

func Column(name string, ct column.Type) func(t *Table) error {
	return func(tbl *Table) error {
		return tbl.block.AddColumn(name, ct)
	}
}
