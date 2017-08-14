package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/kshvakov/clickhouse/internal/protocol"
)

type stmt struct {
	ch       *clickhouse
	query    string
	numInput int
	isInsert bool
	counter  int
}

var emptyResult = &result{}

func (stmt *stmt) NumInput() int {
	switch {
	case stmt.ch.block != nil:
		return len(stmt.ch.block.Columns)
	case stmt.numInput < 0:
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.execContext(context.Background(), args)
}

func (stmt *stmt) execContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	if finish := stmt.ch.watchCancel(ctx); finish != nil {
		defer finish()
	}
	if stmt.isInsert {
		stmt.counter++
		if err := stmt.ch.block.AppendRow(args); err != nil {
			return nil, err
		}
		if (stmt.counter % stmt.ch.blockSize) == 0 {
			if err := stmt.ch.writeBlock(stmt.ch.block); err != nil {
				return nil, err
			}
		}
		return emptyResult, nil
	}
	if err := stmt.ch.sendQuery(stmt.bind(convertOldArgs(args))); err != nil {
		return nil, err
	}
	if err := stmt.ch.wait(); err != nil {
		return nil, err
	}
	return emptyResult, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.queryContext(context.Background(), convertOldArgs(args))
}

func (stmt *stmt) queryContext(ctx context.Context, args []namedValue) (driver.Rows, error) {
	if finish := stmt.ch.watchCancel(ctx); finish != nil {
		defer finish()
	}

	if err := stmt.ch.sendQuery(stmt.bind(args)); err != nil {
		return nil, err
	}

	for {
		packet, err := stmt.ch.decoder.Uvarint()
		if err != nil {
			return nil, err
		}
		switch packet {
		case protocol.ServerData:
			block, err := stmt.ch.readBlock()
			if err != nil {
				return nil, err
			}
			rows := &rows{
				ch:      stmt.ch,
				columns: block.ColumnNames(),
				stream:  make(chan []driver.Value, 1000),
			}
			go rows.receiveData()
			return rows, nil
		case protocol.ServerException:
			return nil, stmt.ch.exception()
		default:
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}

func (stmt *stmt) Close() error {
	stmt.ch.logf("[stmt] close")
	return nil
}

func (stmt *stmt) bind(args []namedValue) string {
	var (
		buf     bytes.Buffer
		index   int
		keyword bool
	)
	switch {
	case stmt.NumInput() != 0:
		reader := bytes.NewReader([]byte(stmt.query))
		for {
			if char, _, err := reader.ReadRune(); err == nil {
				switch char {
				case '@':
					if param := paramParser(reader); len(param) != 0 {
						for _, v := range args {
							if len(v.Name) != 0 && v.Name == param {
								buf.WriteString(quote(v.Value))
							}
						}
					}
				case '?':
					if keyword && index < len(args) && len(args[index].Name) == 0 {
						buf.WriteString(quote(args[index].Value))
						index++
					} else {
						buf.WriteRune(char)
					}
				default:
					switch {
					case
						char == '=',
						char == '<',
						char == '>',
						char == '(',
						char == ',',
						char == '%':
						keyword = true
					default:
						keyword = keyword && (char == ' ' || char == '\t' || char == '\n')
					}
					buf.WriteRune(char)
				}
			} else {
				break
			}
		}
	default:
		buf.WriteString(stmt.query)
	}
	return buf.String()
}

type namedValue struct {
	Name    string
	Ordinal int
	Value   driver.Value
}

func convertOldArgs(args []driver.Value) []namedValue {
	dargs := make([]namedValue, len(args))
	for i, v := range args {
		dargs[i] = namedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return dargs
}
