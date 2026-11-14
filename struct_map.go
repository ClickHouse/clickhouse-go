package clickhouse

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type structMap struct {
	cache sync.Map
}

// structColumnsCache is package-level because StructColumns is a package-level
// helper and has no connection-scoped state to attach cached reflection results
// to.
var structColumnsCache = structColumnCache{}

type structColumnCache struct {
	cache sync.Map
}

func (c *structColumnCache) Load(t reflect.Type) ([]string, bool) {
	columns, ok := c.cache.Load(t)
	if !ok {
		return nil, false
	}
	return copyStrings(columns.([]string)), true
}

func (c *structColumnCache) Store(t reflect.Type, columns []string) {
	c.cache.Store(t, copyStrings(columns))
}

// StructColumns returns the ClickHouse column names represented by v.
//
// StructColumns follows the same field mapping rules as AppendStruct and
// ScanStruct: the `ch` tag overrides the field name, `ch:"-"` omits a field,
// non-anonymous unexported fields are ignored, and non-pointer anonymous
// embedded structs are flattened, and `ch:"name,opt"` uses "name" as the
// column name. Duplicate column names are returned once, in first occurrence
// order.
func StructColumns(v any) ([]string, error) {
	t, err := structType("StructColumns", v)
	if err != nil {
		return nil, err
	}

	if columns, ok := structColumnsCache.Load(t); ok {
		return columns, nil
	}

	columns := structColumns(t)
	structColumnsCache.Store(t, columns)
	return columns, nil
}

func (m *structMap) Map(op string, columns []string, s any, ptr bool) ([]any, error) {
	v := reflect.ValueOf(s)
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
	t := reflect.TypeOf(s)
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
		values = make([]any, 0, len(columns))
	)

	switch idx, found := m.cache.Load(t); {
	case found:
		index = idx.(map[string][]int)
	default:
		index = structIdx(t)
		m.cache.Store(t, index)
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

func structType(op string, v any) (reflect.Type, error) {
	t := reflect.TypeOf(v)
	if t == nil {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("expects a struct or struct pointer"),
		}
	}

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("expects a struct or struct pointer"),
		}
	}

	return t, nil
}

func structColumns(t reflect.Type) []string {
	seen := make(map[string]struct{})
	return appendStructColumns(make([]string, 0, t.NumField()), seen, t)
}

func appendStructColumns(columns []string, seen map[string]struct{}, t reflect.Type) []string {
	for i := 0; i < t.NumField(); i++ {
		var (
			f    = t.Field(i)
			name = f.Name
		)
		if tn := chTagName(f); len(tn) != 0 {
			name = tn
		}
		switch {
		case name == "-", len(f.PkgPath) != 0 && !f.Anonymous:
			continue
		}
		switch {
		case f.Anonymous:
			if f.Type.Kind() == reflect.Struct {
				columns = appendStructColumns(columns, seen, f.Type)
			}
		default:
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			columns = append(columns, name)
		}
	}
	return columns
}

func copyStrings(src []string) []string {
	return append([]string(nil), src...)
}

func chTagName(f reflect.StructField) string {
	if tn := f.Tag.Get("ch"); len(tn) != 0 {
		return strings.Split(tn, ",")[0]
	}
	return ""
}

func structIdx(t reflect.Type) map[string][]int {
	fields := make(map[string][]int)
	for i := 0; i < t.NumField(); i++ {
		var (
			f    = t.Field(i)
			name = f.Name
		)
		if tn := chTagName(f); len(tn) != 0 {
			name = tn
		}
		switch {
		case name == "-", len(f.PkgPath) != 0 && !f.Anonymous:
			continue
		}
		switch {
		case f.Anonymous:
			if f.Type.Kind() == reflect.Struct {
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
