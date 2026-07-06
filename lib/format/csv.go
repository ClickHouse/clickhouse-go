package format

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

const csvNull = `\N`

// CSV implements the ClickHouse CSV format: RFC 4180 double-quote escaping,
// newline-delimited records, NULL rendered as \N. A literal unquoted \N value
// in a String column is indistinguishable from NULL on decode - a limitation
// of the format itself.
type CSV struct{}

func (CSV) Name() string { return "CSV" }

func (CSV) NewEncoder(w io.Writer) Encoder { return &csvEncoder{w: csv.NewWriter(w)} }

func (CSV) NewDecoder(r io.Reader) Decoder { return &csvDecoder{r: r} }

type csvEncoder struct {
	w      *csv.Writer
	record []string
}

func (e *csvEncoder) WriteBlock(block *proto.Block) error {
	rows := block.Rows()
	if cap(e.record) < len(block.Columns) {
		e.record = make([]string, len(block.Columns))
	}
	record := e.record[:len(block.Columns)]
	for row := 0; row < rows; row++ {
		for i, col := range block.Columns {
			value, isNull := renderText(col, row)
			if isNull {
				value = csvNull
			}
			record[i] = value
		}
		if err := e.w.Write(record); err != nil {
			return fmt.Errorf("csv encode: %w", err)
		}
	}
	return nil
}

func (e *csvEncoder) Close() error {
	e.w.Flush()
	if err := e.w.Error(); err != nil {
		return fmt.Errorf("csv encode: %w", err)
	}
	return nil
}

type csvDecoder struct {
	r   io.Reader
	csv *csv.Reader
	row int
}

func (d *csvDecoder) ReadBlock(block *proto.Block, maxRows int) (int, error) {
	if d.csv == nil {
		d.csv = csv.NewReader(d.r)
		d.csv.FieldsPerRecord = len(block.Columns)
		d.csv.ReuseRecord = true
	}
	appended := 0
	for appended < maxRows {
		record, err := d.csv.Read()
		if errors.Is(err, io.EOF) {
			return appended, io.EOF
		}
		if err != nil {
			return appended, fmt.Errorf("csv decode: %w", err)
		}
		d.row++
		for i, col := range block.Columns {
			if record[i] == csvNull {
				if err := col.AppendRow(nil); err != nil {
					return appended, fmt.Errorf("csv decode: row %d: column %s: %w", d.row, col.Name(), err)
				}
				continue
			}
			if err := appendText(col, record[i]); err != nil {
				return appended, fmt.Errorf("csv decode: row %d: column %s: %w", d.row, col.Name(), err)
			}
		}
		appended++
	}
	return appended, nil
}
