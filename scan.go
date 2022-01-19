package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func (ch *clickhouse) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return &OpError{
			Op:  "Select",
			Err: errors.New("must pass a pointer, not a value, to Select destination"),
		}
	}
	if value.IsNil() {
		return &OpError{
			Op:  "Select",
			Err: errors.New("nil pointer passed to Select destination"),
		}
	}
	direct := reflect.Indirect(value)
	if direct.Kind() != reflect.Slice {
		return fmt.Errorf("must pass a slice to Select destination")
	}
	var (
		base      = direct.Type().Elem()
		rows, err = ch.Query(ctx, query, args...)
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		elem := reflect.New(base)
		if err := rows.ScanStruct(elem.Interface()); err != nil {
			return err
		}
		direct.Set(reflect.Append(direct, elem.Elem()))
	}
	return rows.Err()
}

func scan(block *proto.Block, row int, dest ...interface{}) error {
	columns := block.Columns
	if len(columns) != len(dest) {
		return &OpError{
			Op:  "Scan",
			Err: fmt.Errorf("expected %d destination arguments in Scan, not %d", len(columns), len(dest)),
		}
	}
	for i, d := range dest {
		if err := columns[i].ScanRow(d, row-1); err != nil {
			return err
		}
	}
	return nil
}

func structToScannableValues(columns []string, dest interface{}) ([]interface{}, error) {
	var (
		v = reflect.ValueOf(dest)
		t = reflect.TypeOf(dest)
	)
	if v.Kind() != reflect.Ptr {
		return nil, &OpError{
			Op:  "ScanStruct",
			Err: errors.New("must pass a pointer, not a value, to ScanStruct destination"),
		}
	}
	if v.IsNil() {
		return nil, &OpError{
			Op:  "ScanStruct",
			Err: errors.New("nil pointer passed to ScanStruct destination"),
		}
	}
	if v = reflect.Indirect(v); t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var (
		names  = make(map[string]interface{}, len(columns))
		values = make([]interface{}, 0, len(columns))
	)
	for i := 0; i < v.NumField(); i++ {
		var (
			f    = t.Field(i)
			name = f.Name
		)
		if tn := f.Tag.Get("ch"); len(tn) != 0 {
			name = tn
		}
		names[name] = v.Field(i).Addr().Interface()
	}
	for _, name := range columns {
		v, found := names[name]
		if !found {
			return nil, &OpError{
				Op:  "ScanStruct",
				Err: fmt.Errorf("missing destination name %q in %T", name, dest),
			}
		}
		values = append(values, v)
	}
	return values, nil
}
