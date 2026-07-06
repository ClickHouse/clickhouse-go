package format

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// maxJSONLine bounds a single JSONEachRow line on decode.
const maxJSONLine = 16 << 20

// JSONEachRow implements the ClickHouse JSONEachRow format: one JSON object
// per line, keys in column order on encode. Date and DateTime values are
// rendered as ClickHouse text ("2006-01-02 15:04:05"), not RFC 3339.
type JSONEachRow struct{}

func (JSONEachRow) Name() string { return "JSONEachRow" }

func (JSONEachRow) NewEncoder(w io.Writer) Encoder { return &jsonEachRowEncoder{w: w} }

func (JSONEachRow) NewDecoder(r io.Reader) Decoder {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), maxJSONLine)
	return &jsonEachRowDecoder{scanner: scanner}
}

type jsonEachRowEncoder struct {
	w   io.Writer
	buf bytes.Buffer
}

func (e *jsonEachRowEncoder) WriteBlock(block *proto.Block) error {
	rows := block.Rows()
	for row := 0; row < rows; row++ {
		e.buf.Reset()
		e.buf.WriteByte('{')
		for i, col := range block.Columns {
			if i > 0 {
				e.buf.WriteByte(',')
			}
			name, err := json.Marshal(col.Name())
			if err != nil {
				return fmt.Errorf("jsoneachrow encode: column name %q: %w", col.Name(), err)
			}
			e.buf.Write(name)
			e.buf.WriteByte(':')
			value, err := jsonValue(col, row)
			if err != nil {
				return fmt.Errorf("jsoneachrow encode: column %s: %w", col.Name(), err)
			}
			e.buf.Write(value)
		}
		e.buf.WriteString("}\n")
		if _, err := e.w.Write(e.buf.Bytes()); err != nil {
			return fmt.Errorf("jsoneachrow encode: %w", err)
		}
	}
	return nil
}

func (e *jsonEachRowEncoder) Close() error { return nil }

func jsonValue(col column.Interface, row int) ([]byte, error) {
	v, isNull := rowValue(col, row)
	if isNull {
		return []byte("null"), nil
	}
	if t, ok := v.(time.Time); ok {
		return json.Marshal(t.Format(timeLayout(col.Type())))
	}
	return json.Marshal(v)
}

type jsonEachRowDecoder struct {
	scanner *bufio.Scanner
	row     int
}

func (d *jsonEachRowDecoder) ReadBlock(block *proto.Block, maxRows int) (int, error) {
	appended := 0
	for appended < maxRows {
		if !d.scanner.Scan() {
			if err := d.scanner.Err(); err != nil {
				return appended, fmt.Errorf("jsoneachrow decode: %w", err)
			}
			return appended, io.EOF
		}
		line := bytes.TrimSpace(d.scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		d.row++
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(line, &fields); err != nil {
			return appended, fmt.Errorf("jsoneachrow decode: row %d: %w", d.row, err)
		}
		for _, col := range block.Columns {
			if err := d.appendField(col, fields[col.Name()]); err != nil {
				return appended, fmt.Errorf("jsoneachrow decode: row %d: column %s: %w", d.row, col.Name(), err)
			}
		}
		appended++
	}
	return appended, nil
}

// appendField appends one JSON value to col. Missing keys and JSON null both
// append the column default (NULL for Nullable columns).
func (d *jsonEachRowDecoder) appendField(col column.Interface, raw json.RawMessage) error {
	if raw == nil || bytes.Equal(raw, []byte("null")) {
		return col.AppendRow(nil)
	}
	st := col.ScanType()
	if st == nil {
		return fmt.Errorf("type %s not supported by client-side JSONEachRow codec; use the HTTP protocol", col.Type())
	}
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}
	if st == timeType {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		t, err := parseTextTime(col.Type(), s)
		if err != nil {
			return err
		}
		return col.AppendRow(t)
	}
	target := reflect.New(st)
	if err := json.Unmarshal(raw, target.Interface()); err != nil {
		return err
	}
	return col.AppendRow(target.Elem().Interface())
}
