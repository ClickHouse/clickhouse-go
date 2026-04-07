package driver

import (
	"errors"
	"io"
	"reflect"
	"testing"
)

type testRows struct {
	values          []int
	index           int
	closeCalls      int
	err             error
	scanStructErrAt int
}

func (r *testRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}
	r.index++
	return true
}

func (r *testRows) Scan(dest ...any) error { return nil }

func (r *testRows) ScanStruct(dest any) error {
	if r.scanStructErrAt > 0 && r.index == r.scanStructErrAt {
		return io.ErrUnexpectedEOF
	}
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return errors.New("expected pointer to struct")
	}
	field := value.Elem().FieldByName("Value")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Int {
		return errors.New("expected struct with settable int Value field")
	}
	field.SetInt(int64(r.values[r.index-1]))
	return nil
}

func (r *testRows) ColumnTypes() []ColumnType { return nil }

func (r *testRows) Totals(dest ...any) error { return nil }

func (r *testRows) Columns() []string { return nil }

func (r *testRows) Close() error {
	r.closeCalls++
	return nil
}

func (r *testRows) Err() error { return r.err }

func (r *testRows) HasData() bool { return r.index < len(r.values) }

func TestStructIter(t *testing.T) {
	type item struct {
		Value int
	}

	rows := &testRows{values: []int{4, 5, 6}}

	var got []int
	for value, err := range StructIter[item](rows) {
		if err != nil {
			t.Fatalf("unexpected iter error: %v", err)
		}
		got = append(got, value.Value)
	}

	if !reflect.DeepEqual(got, []int{4, 5, 6}) {
		t.Fatalf("unexpected values: %#v", got)
	}
	if rows.closeCalls == 0 {
		t.Fatal("expected rows to be closed")
	}
}

func TestStructIterScanError(t *testing.T) {
	type item struct {
		Value int
	}

	rows := &testRows{values: []int{7, 8, 9}, scanStructErrAt: 2}

	var got []int
	var gotErr error
	for value, err := range StructIter[item](rows) {
		if err != nil {
			gotErr = err
			break
		}
		got = append(got, value.Value)
	}

	if !errors.Is(gotErr, io.ErrUnexpectedEOF) {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if !reflect.DeepEqual(got, []int{7}) {
		t.Fatalf("unexpected values before error: %#v", got)
	}
}

func TestStructIterTerminalRowsError(t *testing.T) {
	type item struct {
		Value int
	}

	rows := &testRows{values: []int{1}, err: io.EOF}

	var got []item
	var gotErr error
	for value, err := range StructIter[item](rows) {
		if err != nil {
			gotErr = err
			break
		}
		got = append(got, value)
	}

	if !errors.Is(gotErr, io.EOF) {
		t.Fatalf("unexpected terminal error: %v", gotErr)
	}
	if !reflect.DeepEqual(got, []item{{Value: 1}}) {
		t.Fatalf("unexpected values before terminal error: %#v", got)
	}
}
