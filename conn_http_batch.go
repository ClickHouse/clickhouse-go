// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func (h *httpConnect) fetchColumnNamesAndTypesForInsert(ctx context.Context, tableName string, requestedColumnNames []string) ([]ColumnNameAndType, error) {
	describeTableQuery := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	r, err := h.query(ctx, nil, describeTableQuery)
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
			colType, ok := columnsToTypes[colName]
			if !ok {
				return nil, fmt.Errorf("column %s is not present in the table %s", colName, tableName)
			}

			insertColumns = append(insertColumns, ColumnNameAndType{
				Name: colName,
				Type: colType,
			})
		}
	}

	return insertColumns, nil
}

func (h *httpConnect) newBlock(ctx context.Context, query string) (string, *proto.Block, error) {
	normalizedQuery, tableName, requestedColumnNames, err := extractNormalizedInsertQueryAndColumns(query)
	if err != nil {
		return "", nil, err
	}

	opt := queryOptions(ctx)
	columns := opt.columnNamesAndTypes

	// If the user didn't supply known column names/types, do expensive DESC TABLE logic
	if opt.columnNamesAndTypes == nil {
		fetchedColumns, err := h.fetchColumnNamesAndTypesForInsert(ctx, tableName, requestedColumnNames)
		if err != nil {
			return "", nil, fmt.Errorf("failed to determine columns for HTTP insert: %w", err)
		}
		columns = fetchedColumns
	}

	var block proto.Block
	for _, col := range columns {
		if err := block.AddColumn(col.Name, column.Type(col.Type)); err != nil {
			return "", nil, err
		}
	}

	return normalizedQuery, &block, nil
}

// release is ignored, because http used by std with empty release function.
// Also opts ignored because all options unused in http batch.
func (h *httpConnect) prepareBatch(ctx context.Context, query string, opts driver.PrepareBatchOptions, release func(*connect, error), acquire func(context.Context) (*connect, error)) (driver.Batch, error) {
	query, block, err := h.newBlock(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to init block for HTTP batch: %w", err)
	}

	return &httpBatch{
		ctx:       ctx,
		conn:      h,
		structMap: &structMap{},
		block:     block,
		query:     query,
	}, nil
}

type httpBatch struct {
	query     string
	err       error
	ctx       context.Context
	conn      *httpConnect
	structMap *structMap
	sent      bool
	block     *proto.Block
}

func (b *httpBatch) Flush() error {
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}
	if b.block.Rows() == 0 {
		return nil
	}

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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error = nil
		defer pipeWriter.CloseWithError(err)
		defer connWriter.Close()
		b.conn.buffer.Reset()
		if b.block.Rows() != 0 {
			if err = b.conn.writeData(b.block); err != nil {
				return
			}
		}
		if err = b.conn.writeData(&proto.Block{}); err != nil {
			return
		}
		if _, err = connWriter.Write(b.conn.buffer.Buf); err != nil {
			return
		}
	}()

	options.settings["query"] = b.query
	headers["Content-Type"] = "application/octet-stream"

	res, err := b.conn.sendStreamQuery(b.ctx, pipeReader, &options, headers)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// TODO: Is the connection being leaked here?
	// Something about discarding the body causes this to break
	// HTTP flushing, but may leak connections if body isn't fully read.
	// The goroutine above this seems to handle this. See flush_test.go for example.
	//defer discardAndClose(res.Body)

	b.block.Reset()
	wg.Wait()

	return nil
}

func (b *httpBatch) Close() error {
	b.sent = true
	return nil
}

func (b *httpBatch) Abort() error {
	defer func() {
		b.sent = true
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
	if err := b.block.Append(v...); err != nil {
		return err
	}
	return nil
}

func (b *httpBatch) AppendStruct(v any) error {
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
	}()

	return b.Flush()
}

func (b *httpBatch) Rows() int {
	return b.block.Rows()
}

func (b *httpBatch) Columns() []column.Interface {
	return slices.Clone(b.block.Columns)
}

var _ driver.Batch = (*httpBatch)(nil)
