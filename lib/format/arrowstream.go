package format

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// ArrowStream implements the ClickHouse ArrowStream format (the Arrow IPC
// streaming format). Unlike Parquet it is fully streamable in both
// directions: each block becomes one record batch. Input columns are matched
// to table columns by name.
type ArrowStream struct{}

func (ArrowStream) Name() string { return "ArrowStream" }

func (ArrowStream) NewEncoder(w io.Writer) Encoder { return &arrowStreamEncoder{w: w} }

func (ArrowStream) NewDecoder(r io.Reader) Decoder { return &arrowStreamDecoder{r: r} }

type arrowStreamEncoder struct {
	w       io.Writer
	wr      *ipc.Writer
	builder *array.RecordBuilder
}

func (e *arrowStreamEncoder) WriteBlock(block *proto.Block) error {
	if e.wr == nil {
		schema, err := blockArrowSchema("ArrowStream", block)
		if err != nil {
			return fmt.Errorf("arrowstream encode: %w", err)
		}
		e.wr = ipc.NewWriter(e.w, ipc.WithSchema(schema))
		e.builder = array.NewRecordBuilder(memory.DefaultAllocator, schema)
	}
	if block.Rows() == 0 {
		return nil
	}
	rec, err := buildRecordBatch("ArrowStream", e.builder, block)
	if err != nil {
		return fmt.Errorf("arrowstream encode: %w", err)
	}
	defer rec.Release()
	if err := e.wr.Write(rec); err != nil {
		return fmt.Errorf("arrowstream encode: %w", err)
	}
	return nil
}

func (e *arrowStreamEncoder) Close() error {
	if e.wr == nil {
		return nil
	}
	e.builder.Release()
	// ipc.Writer.Close writes the end-of-stream marker; it does not close the
	// underlying writer.
	if err := e.wr.Close(); err != nil {
		return fmt.Errorf("arrowstream encode: %w", err)
	}
	return nil
}

type arrowStreamDecoder struct {
	r      io.Reader
	rdr    *ipc.Reader
	stream *recordStreamDecoder
}

func (d *arrowStreamDecoder) ReadBlock(block *proto.Block, maxRows int) (int, error) {
	if d.stream == nil {
		rdr, err := ipc.NewReader(d.r)
		if err != nil {
			return 0, fmt.Errorf("arrowstream decode: %w", err)
		}
		d.rdr = rdr
		d.stream = &recordStreamDecoder{
			codecName: "ArrowStream",
			next: func() (arrow.RecordBatch, error) {
				if !rdr.Next() {
					if err := rdr.Err(); err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					return nil, io.EOF
				}
				rec := rdr.RecordBatch()
				rec.Retain() // recordStreamDecoder owns and releases it
				return rec, nil
			},
		}
	}
	return d.stream.ReadBlock(block, maxRows)
}
