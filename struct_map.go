package clickhouse

import (
	"fmt"
	"reflect"
)

type structMap struct {
	cache map[reflect.Type]map[string][]int
}

func (m *structMap) Map(op string, columns []string, s interface{}, ptr bool) ([]interface{}, error) {
	var (
		v = reflect.ValueOf(s)
		t = reflect.TypeOf(s)
	)
	if v.Kind() != reflect.Ptr {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("must pass a pointer, not a value, to %s destination", op),
		}
	}
	if v.IsNil() {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("nil pointer passed to %s destination", op),
		}
	}
	if v = reflect.Indirect(v); t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("%s expects a struct dest", op),
		}
	}
	var (
		index  map[string][]int
		values = make([]interface{}, 0, len(columns))
	)

	switch idx, found := m.cache[t]; {
	case found:
		index = idx
	default:
		index = structIdx(t)
		m.cache[t] = index
	}
	for _, name := range columns {
		idx, found := index[name]
		if !found {
			return nil, &OpError{
				Op:  op,
				Err: fmt.Errorf("missing destination name %q in %T", name, s),
			}
		}
		switch field := v.FieldByIndex(idx); {
		case ptr:
			values = append(values, field.Addr().Interface())
		default:
			values = append(values, field.Interface())
		}
	}
	return values, nil
}

func structIdx(t reflect.Type) map[string][]int {
	fields := make(map[string][]int)
	for i := 0; i < t.NumField(); i++ {
		var (
			f    = t.Field(i)
			name = f.Name
		)
		if tn := f.Tag.Get("ch"); len(tn) != 0 {
			name = tn
		}
		switch {
		case name == "-", len(f.PkgPath) != 0 && !f.Anonymous:
			continue
		}
		switch {
		case f.Anonymous:
			if f.Type.Kind() != reflect.Ptr {
				for k, idx := range structIdx(f.Type) {
					fields[k] = append(f.Index, idx...)
				}
			}
		default:
			fields[name] = f.Index
		}
	}
	return fields
}
