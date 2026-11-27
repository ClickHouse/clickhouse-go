package ext

import (
	"testing"
)

func TestTableStructure(t *testing.T) {
	tests := []struct {
		name    string
		columns []func(t *Table) error
		want    string
	}{
		{
			name: "standard columns",
			columns: []func(t *Table) error{
				Column("col1", "UInt8"),
				Column("col2", "String"),
			},
			want: "`col1` UInt8, `col2` String",
		},
		{
			name: "spaces commas parens and non-ascii",
			columns: []func(t *Table) error{
				Column("test id", "UInt64"),
				Column("a,b", "UInt64"),
				Column("f(x)", "UInt64"),
				Column("é", "UInt64"),
			},
			want: "`test id` UInt64, `a,b` UInt64, `f(x)` UInt64, `é` UInt64",
		},
		{
			name: "embedded and outer backticks",
			columns: []func(t *Table) error{
				Column("back`tick", "UInt64"),
				Column("`test id`", "UInt64"),
			},
			want: "`back\\`tick` UInt64, `\\`test id\\`` UInt64",
		},
		{
			name: "backslash and declaration text",
			columns: []func(t *Table) error{
				Column(`a\b`, "UInt64"),
				Column("x UInt8, y", "UInt64"),
			},
			want: "`a\\\\b` UInt64, `x UInt8, y` UInt64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl, err := NewTable("ext", tt.columns...)
			if err != nil {
				t.Fatalf("NewTable failed: %s", err)
			}
			if got := tbl.Structure(); got != tt.want {
				t.Errorf("Structure() = %q, want %q", got, tt.want)
			}
		})
	}
}
