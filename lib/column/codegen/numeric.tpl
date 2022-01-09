package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
)
type (
{{- range . }}
	{{ .Type }} []{{ .GoType }}
{{- end }}
)
var (
{{- range . }}
	_ Interface = (*{{ .Type }})(nil)
{{- end }}
)

{{- range . }}

func (col *{{ .Type }}) Rows() int {
	return len(*col)
}

func (col *{{ .Type }}) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *{{ .GoType }}:
		*d = value[row]
	case **{{ .GoType }}:
		*d = new({{ .GoType }})
		**d = value[row]
	default:
		return fmt.Errorf("converting {{ .Type }} to %T is unsupported", d)
	}
	return nil
}

func (col *{{ .Type }}) RowValue(row int) interface{} {
	value := *col
	return value[row]
}

func (col *{{ .Type }}) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case {{ .GoType }}:
		*col = append(*col, v)
	case null:
		*col = append(*col, 0)
	}
	return nil
}

func (col *{{ .Type }}) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.{{ .Type }}()
		if err != nil {
			return err
		}
		*col = append(*col, v)
	}
	return nil
}

func (col *{{ .Type }}) Encode(encoder *binary.Encoder) error {
	for _, v := range *col {
		if err := encoder.{{ .Type }}(v); err != nil {
			return err
		}
	}
	return nil
}

{{- end }}