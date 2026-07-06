package format

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Parquet implements the ClickHouse Parquet format via arrow-go, mirroring
// the server, which implements Parquet through the Arrow C++ library.
//
// Encoding streams: each block becomes a row group and Close writes the file
// footer. Decoding cannot stream - the Parquet metadata footer sits at the end
// of the file - so the whole payload is buffered in memory first; for very
// large files prefer the HTTP protocol, where the server parses the upload.
// Input columns are matched to table columns by name.
type Parquet struct{}

func (Parquet) Name() string { return "Parquet" }

func (Parquet) NewEncoder(w io.Writer) Encoder { return &parquetEncoder{w: w} }

func (Parquet) NewDecoder(r io.Reader) Decoder { return &parquetDecoder{r: r} }

type parquetEncoder struct {
	w       io.Writer
	fw      *pqarrow.FileWriter
	builder *array.RecordBuilder
}

func (e *parquetEncoder) WriteBlock(block *proto.Block) error {
	if e.fw == nil {
		schema, err := blockArrowSchema("Parquet", block)
		if err != nil {
			return fmt.Errorf("parquet encode: %w", err)
		}
		// The parquet writer closes its sink on Close, but Encoder must not
		// close the underlying writer - hide any Close method.
		fw, err := pqarrow.NewFileWriter(schema, struct{ io.Writer }{e.w}, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
		if err != nil {
			return fmt.Errorf("parquet encode: %w", err)
		}
		e.fw = fw
		e.builder = array.NewRecordBuilder(memory.DefaultAllocator, schema)
	}
	if block.Rows() == 0 {
		return nil
	}
	rec, err := buildRecordBatch("Parquet", e.builder, block)
	if err != nil {
		return fmt.Errorf("parquet encode: %w", err)
	}
	defer rec.Release()
	if err := e.fw.Write(rec); err != nil {
		return fmt.Errorf("parquet encode: %w", err)
	}
	return nil
}

func (e *parquetEncoder) Close() error {
	if e.fw == nil {
		return nil
	}
	e.builder.Release()
	if err := e.fw.Close(); err != nil {
		return fmt.Errorf("parquet encode: %w", err)
	}
	return nil
}

type parquetDecoder struct {
	r      io.Reader
	rr     pqarrow.RecordReader
	stream *recordStreamDecoder
}

func (d *parquetDecoder) ReadBlock(block *proto.Block, maxRows int) (int, error) {
	if d.stream == nil {
		if err := d.init(maxRows); err != nil {
			return 0, fmt.Errorf("parquet decode: %w", err)
		}
	}
	return d.stream.ReadBlock(block, maxRows)
}

func (d *parquetDecoder) init(batchSize int) error {
	// Parquet needs random access to reach the trailing footer, so the
	// payload is buffered in memory.
	data, err := io.ReadAll(d.r)
	if err != nil {
		return err
	}
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return err
	}
	d.rr = rr
	d.stream = &recordStreamDecoder{
		codecName: "Parquet",
		next: func() (arrow.RecordBatch, error) {
			if !rr.Next() {
				if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
					return nil, err
				}
				return nil, io.EOF
			}
			rec := rr.RecordBatch()
			rec.Retain() // recordStreamDecoder owns and releases it
			return rec, nil
		},
	}
	return nil
}
