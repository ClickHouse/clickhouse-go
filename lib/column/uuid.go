package column

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/ClickHouse/ch-go/proto"
	guuid "github.com/google/uuid"

	"uuid"
)

type UUID struct {
	col  proto.ColUUID
	name string
}

func (col *UUID) Reset() {
	col.col.Reset()
}

func (col *UUID) Name() string {
	return col.name
}

func (col *UUID) Type() Type {
	return "UUID"
}

func (col *UUID) ScanType() reflect.Type {
	return scanTypeUUID
}

func (col *UUID) Rows() int {
	return col.col.Rows()
}

func (col *UUID) Row(i int, ptr bool) any {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UUID) ScanRow(dest any, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row).String()
	case **string:
		*d = new(string)
		**d = col.row(row).String()
	case *uuid.UUID:
		*d = col.row(row)
	case **uuid.UUID:
		*d = new(uuid.UUID)
		**d = col.row(row)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(col.row(row).String())
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UUID",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *UUID) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			var u uuid.UUID
			u, err = uuid.Parse(v)
			if err != nil {
				return
			}
			col.appendToCol(u)
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				var value uuid.UUID
				value, err = uuid.Parse(*v)
				if err != nil {
					return
				}
				col.appendToCol(value)
			default:
				nulls[i] = 1
				col.appendToCol(uuid.UUID{})
			}
		}
	case []uuid.UUID:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.appendToCol(v)
		}
	case []*uuid.UUID:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.appendToCol(*v)
			default:
				nulls[i] = 1
				col.appendToCol(uuid.UUID{})
			}
		}
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "UUID",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UUID",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UUID) AppendRow(v any) error {
	switch v := v.(type) {
	case string:
		u, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		col.appendToCol(u)
	case *string:
		switch {
		case v != nil:
			value, err := uuid.Parse(*v)
			if err != nil {
				return err
			}
			col.appendToCol(value)
		default:
			col.appendToCol(uuid.UUID{})
		}
	case uuid.UUID:
		col.appendToCol(v)
	case *uuid.UUID:
		switch {
		case v != nil:
			col.appendToCol(*v)
		default:
			col.appendToCol(uuid.UUID{})
		}
	case nil:
		col.appendToCol(uuid.UUID{})
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "UUID",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}
		if s, ok := v.(fmt.Stringer); ok {
			return col.AppendRow(s.String())
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UUID",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UUID) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *UUID) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *UUID) row(i int) (u uuid.UUID) {
	return uuid.UUID(col.col.Row(i))
}

func (col *UUID) appendToCol(u uuid.UUID) {
	col.col.Append(guuid.UUID(u))
}

var _ Interface = (*UUID)(nil)
