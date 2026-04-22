package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func fetchColumnNamesAndTypesForInsert(h *httpConnect, release nativeTransportRelease, ctx context.Context, tableName string, requestedColumnNames []string) ([]ColumnNameAndType, error) {
	describeTableQuery := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	r, err := h.query(ctx, release, describeTableQuery)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	columnsToTypes := make(map[string]string)
	var allColumns []string
	for r.Next() {
		var (
			colName     string
			colType     string
			defaultType string
			ignore      string
		)

		if err = r.Scan(&colName, &colType, &defaultType, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		// these column types cannot be specified in INSERT queries
		if defaultType == "MATERIALIZED" || defaultType == "ALIAS" {
			continue
		}

		columnsToTypes[colName] = colType
		allColumns = append(allColumns, colName)
	}

	// The order of the columns must match the INSERT list, or the DESC table if no insert list was provided
	insertColumns := make([]ColumnNameAndType, 0, len(allColumns))

	if len(requestedColumnNames) > 0 {
		// Validate requested columns present
		for _, colName := range requestedColumnNames {
			colType, ok := columnsToTypes[colName]
			if !ok {
				return nil, fmt.Errorf("column %s is not present in the table %s", colName, tableName)
			}

			insertColumns = append(insertColumns, ColumnNameAndType{
				Name: colName,
				Type: colType,
			})
		}
	} else {
		// Use all columns
		for _, colName := range allColumns {
			colType := columnsToTypes[colName]
			insertColumns = append(insertColumns, ColumnNameAndType{
				Name: colName,
				Type: colType,
			})
		}
	}

	return insertColumns, nil
}

func newBlock(h *httpConnect, release nativeTransportRelease, ctx context.Context, query string, format driver.InsertFormat) (string, *proto.Block, error) {
	normalizedQuery, tableName, requestedColumnNames, err := extractNormalizedInsertQueryAndColumnsWithFormat(query, format)
	if err != nil {
		return "", nil, err
	}

	opt := queryOptions(ctx)
	columns := opt.columnNamesAndTypes

	// If the user didn't supply known column names/types, do expensive DESC TABLE logic
	if opt.columnNamesAndTypes == nil {
		fetchedColumns, err := fetchColumnNamesAndTypesForInsert(h, release, ctx, tableName, requestedColumnNames)
		if err != nil {
			return "", nil, fmt.Errorf("failed to determine columns for HTTP insert: %w", err)
		}
		columns = fetchedColumns
	}

	var block proto.Block
	serverContext := serverVersionToContext(h.handshake)
	block.ServerContext = &serverContext
	for _, col := range columns {
		if err := block.AddColumn(col.Name, column.Type(col.Type)); err != nil {
			return "", nil, err
		}
	}

	return normalizedQuery, &block, nil
}

func (h *httpConnect) prepareBatch(ctx context.Context, release nativeTransportRelease, acquire nativeTransportAcquire, query string, opts driver.PrepareBatchOptions) (driver.Batch, error) {
	// Resolve the effective insert format: per-batch option > connection-level option > default (Native)
	format := h.opt.InsertFormat
	if opts.InsertFormat != "" {
		format = opts.InsertFormat
	}

	// release is not used within newBlock since the connection is held for the batch.
	query, block, err := newBlock(h, func(nativeTransport, error) {}, ctx, query, format)
	if err != nil {
		err = fmt.Errorf("failed to init block for HTTP batch: %w", err)
		release(h, err)
		return nil, err
	}

	return &httpBatch{
		ctx:          ctx,
		conn:         h,
		connRelease:  release,
		structMap:    &structMap{},
		block:        block,
		query:        query,
		insertFormat: format,
	}, nil
}

type httpBatch struct {
	query        string
	err          error
	ctx          context.Context
	conn         *httpConnect
	released     bool
	connRelease  nativeTransportRelease
	structMap    *structMap
	sent         bool
	block        *proto.Block
	insertFormat driver.InsertFormat
}

func (b *httpBatch) release(err error) {
	if !b.released {
		b.released = true
		b.connRelease(b.conn, err)
	}
}

func (b *httpBatch) Flush() error {
	// Flush and Send are effectively the same for HTTP, but users should just use Send until we
	// figure out a way to do proper streaming.
	return nil
}

func (b *httpBatch) Close() error {
	if b.sent || b.released {
		return nil
	}

	b.sent = true
	b.release(nil)

	return nil
}

func (b *httpBatch) Abort() error {
	defer func() {
		b.sent = true
		b.release(os.ErrProcessDone)
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	return nil
}

func (b *httpBatch) Append(v ...any) error {
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}

	if err := b.block.Append(v...); err != nil {
		b.err = fmt.Errorf("%w: %w", ErrBatchInvalid, err)
		b.release(err)
		return err
	}

	return nil
}

func (b *httpBatch) AppendStruct(v any) error {
	if b.err != nil {
		return b.err
	}
	values, err := b.structMap.Map("AppendStruct", b.block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}

func (b *httpBatch) Column(idx int) driver.BatchColumn {
	if len(b.block.Columns) <= idx {
		return &batchColumn{
			err: &OpError{
				Op:  "batch.Column",
				Err: fmt.Errorf("invalid column index %d", idx),
			},
		}
	}
	return &batchColumn{
		batch:  b,
		column: b.block.Columns[idx],
		release: func(err error) {
			b.err = err
		},
	}
}

func (b *httpBatch) IsSent() bool {
	return b.sent
}

func (b *httpBatch) Send() (err error) {
	defer func() {
		b.sent = true
		b.release(err)
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}
	if b.block.Rows() == 0 {
		return nil
	}

	if b.insertFormat == driver.InsertFormatJSONEachRow {
		return b.sendJSONEachRow()
	}
	return b.sendNative()
}

// sendNative sends batch data using FORMAT Native (binary columnar blocks).
func (b *httpBatch) sendNative() error {
	options := queryOptions(b.ctx)
	headers := make(map[string]string)
	switch b.conn.compression {
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		headers["Content-Encoding"] = b.conn.compression.String()
	case CompressionZSTD, CompressionLZ4:
		options.settings["decompress"] = "1"
		options.settings["compress"] = "1"
	}

	compressionWriter := b.conn.compressionPool.Get()
	defer b.conn.compressionPool.Put(compressionWriter)
	pipeReader, pipeWriter := io.Pipe()
	connWriter := compressionWriter.reset(pipeWriter)

	go func() {
		var err error
		defer pipeWriter.CloseWithError(err)
		defer connWriter.Close()
		b.conn.buffer.Reset()
		if err = b.conn.writeData(b.block); err != nil {
			return
		}
		if _, err = connWriter.Write(b.conn.buffer.Buf); err != nil {
			return
		}
	}()

	options.settings["query"] = b.query
	headers["Content-Type"] = "application/octet-stream"

	b.conn.logger.Debug("batch: sending via HTTP (Native)",
		slog.Int("columns", len(b.block.Columns)),
		slog.Int("rows", b.block.Rows()))
	res, err := b.conn.sendStreamQuery(b.ctx, pipeReader, &options, headers) //nolint:bodyclose // false positive
	if err != nil {
		return fmt.Errorf("batch sendStreamQuery: %w", err)
	}
	discardAndClose(res.Body)

	b.conn.logger.Debug("batch: send complete")
	b.block.Reset()

	return nil
}

// sendJSONEachRow sends batch data using FORMAT JSONEachRow (newline-delimited JSON).
// This format is compatible with ClickHouse async_insert buffering.
func (b *httpBatch) sendJSONEachRow() error {
	options := queryOptions(b.ctx)
	headers := make(map[string]string)
	switch b.conn.compression {
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		headers["Content-Encoding"] = b.conn.compression.String()
	}

	// Serialize block rows to JSONEachRow format
	jsonData, err := blockToJSONEachRow(b.block)
	if err != nil {
		return fmt.Errorf("batch JSON serialization: %w", err)
	}

	var body io.Reader
	if b.conn.compression == CompressionGZIP || b.conn.compression == CompressionDeflate || b.conn.compression == CompressionBrotli {
		compressionWriter := b.conn.compressionPool.Get()
		defer b.conn.compressionPool.Put(compressionWriter)
		pipeReader, pipeWriter := io.Pipe()
		connWriter := compressionWriter.reset(pipeWriter)
		go func() {
			defer pipeWriter.CloseWithError(err)
			defer connWriter.Close()
			_, err = connWriter.Write(jsonData)
		}()
		body = pipeReader
	} else {
		body = bytes.NewReader(jsonData)
	}

	options.settings["query"] = b.query
	headers["Content-Type"] = "text/plain"

	b.conn.logger.Debug("batch: sending via HTTP (JSONEachRow)",
		slog.Int("columns", len(b.block.Columns)),
		slog.Int("rows", b.block.Rows()),
		slog.Int("bytes", len(jsonData)))
	res, err := b.conn.sendStreamQuery(b.ctx, body, &options, headers) //nolint:bodyclose // false positive
	if err != nil {
		return fmt.Errorf("batch sendStreamQuery: %w", err)
	}
	discardAndClose(res.Body)

	b.conn.logger.Debug("batch: send complete")
	b.block.Reset()

	return nil
}

// blockToJSONEachRow converts a proto.Block to JSONEachRow format (newline-delimited JSON objects).
// Each row becomes a JSON object: {"col1": val1, "col2": val2, ...}\n
func blockToJSONEachRow(block *proto.Block) ([]byte, error) {
	numRows := block.Rows()
	numCols := len(block.Columns)
	if numRows == 0 || numCols == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	colNames := block.ColumnsNames()

	for row := 0; row < numRows; row++ {
		rowMap := make(map[string]any, numCols)
		for col := 0; col < numCols; col++ {
			val := block.Columns[col].Row(row, false)
			rowMap[colNames[col]] = formatJSONValue(val)
		}
		jsonLine, err := json.Marshal(rowMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal row %d: %w", row, err)
		}
		buf.Write(jsonLine)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// formatJSONValue converts column values to JSON-compatible types.
// ClickHouse JSONEachRow expects specific formats for certain types.
func formatJSONValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case fmt.Stringer:
		// Handle types like time.Time, net.IP, uuid, etc. that implement Stringer
		s := val.String()
		// Avoid wrapping simple numeric strings
		if _, ok := v.(interface{ Unix() int64 }); ok {
			return s
		}
		// Check if it looks like it should remain as-is (maps, slices are handled by json.Marshal)
		if strings.HasPrefix(s, "[") || strings.HasPrefix(s, "{") {
			return v
		}
		return s
	default:
		return v
	}
}

func (b *httpBatch) Rows() int {
	return b.block.Rows()
}

func (b *httpBatch) Columns() []column.Interface {
	return slices.Clone(b.block.Columns)
}

var _ driver.Batch = (*httpBatch)(nil)
