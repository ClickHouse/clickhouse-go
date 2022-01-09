
package column

import (
	"strings"
)

func (t Type) Column() (Interface,error) {
	if t.IsNullable() {
		base,err:=t.Base().Column()
		if err != nil {
			return nil, err
		}
		return &Nullable{
			base: base,
		}, nil
	}
	switch t {
{{- range . }}
	case "{{ .Type }}":
		return &{{ .Type }}{}, nil
{{- end }}
	case "String":
		return &String{}, nil
	case "DateTime":
		return &DateTime{}, nil
	}
	if strings.HasPrefix(string(t), "Enum") {
		return Enum(string(t))
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}
