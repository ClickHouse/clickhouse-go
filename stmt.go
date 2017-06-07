package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
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
	if stmt.numInput < 0 {
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
		if err := stmt.ch.data.append(args); err != nil {
			return nil, err
		}
		if (stmt.counter % stmt.ch.blockSize) == 0 {
			if err := stmt.ch.data.write(stmt.ch.serverRevision, stmt.ch.conn); err != nil {
				return nil, err
			}
		}
		return emptyResult, nil
	}
	if err := stmt.ch.sendQuery(stmt.query); err != nil {
		return nil, err
	}
	if _, err := stmt.ch.receiveData(); err != nil {
		return nil, err
	}
	return emptyResult, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.queryContext(context.Background(), convertOldArgs(args))
}

func (stmt *stmt) queryContext(ctx context.Context, args []namedValue) (driver.Rows, error) {
	var (
		buf     bytes.Buffer
		index   int
		keyword bool
	)

	if finish := stmt.ch.watchCancel(ctx); finish != nil {
		defer finish()
	}

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

	if err := stmt.ch.sendQuery(buf.String()); err != nil {
		return nil, err
	}

	rows, err := stmt.ch.receiveData()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (stmt *stmt) Close() error {
	stmt.ch.logf("[stmt] close")
	return nil
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
