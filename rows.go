package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/kshvakov/clickhouse/lib/column"
	"github.com/kshvakov/clickhouse/lib/data"
	"github.com/kshvakov/clickhouse/lib/protocol"
)

type rows struct {
	ch                *clickhouse
	err               error
	finish            func()
	values            [][]driver.Value
	totals            [][]driver.Value
	extremes          [][]driver.Value
	stream            chan []driver.Value
	columns           []string
	blockColumns      []column.Column
	allDataIsReceived int32
}

func (rows *rows) Columns() []string {
	return rows.columns
}

func (rows *rows) ColumnTypeScanType(idx int) reflect.Type {
	return rows.blockColumns[idx].ScanType()
}

func (rows *rows) ColumnTypeDatabaseTypeName(idx int) string {
	return rows.blockColumns[idx].CHType()
}

func (rows *rows) Next(dest []driver.Value) error {
	row, ok := <-rows.stream
	if !ok {
		if rows.err != nil {
			return rows.err
		}
		return io.EOF
	}
	for i := range dest {
		dest[i] = row[i]
	}
	return nil
}

func (rows *rows) HasNextResultSet() bool {
	return len(rows.totals) != 0 || len(rows.extremes) != 0
}

func (rows *rows) NextResultSet() error {
	switch {
	case len(rows.totals) != 0:
		rows.stream = make(chan []driver.Value, len(rows.totals)+1)
		for _, value := range rows.totals {
			rows.stream <- value
		}
		close(rows.stream)
		rows.totals = nil
	case len(rows.extremes) != 0:
		rows.stream = make(chan []driver.Value, len(rows.extremes)+1)
		for _, value := range rows.extremes {
			rows.stream <- value
		}
		close(rows.stream)
		rows.extremes = nil
	default:
		return io.EOF
	}
	return nil
}

func (rows *rows) receiveData() error {
	defer func() {
		close(rows.stream)
		atomic.StoreInt32(&rows.allDataIsReceived, 1)
	}()
	var (
		packet      uint64
		progress    *progress
		profileInfo *profileInfo
	)
	for {
		if packet, rows.err = rows.ch.decoder.Uvarint(); rows.err != nil {
			return rows.err
		}
		switch packet {
		case protocol.ServerException:
			rows.ch.logf("[rows] <- exception")
			return rows.ch.exception()
		case protocol.ServerProgress:
			if progress, rows.err = rows.ch.progress(); rows.err != nil {
				return rows.err
			}
			rows.ch.logf("[rows] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.rows,
				progress.bytes,
				progress.totalRows,
			)
		case protocol.ServerProfileInfo:
			if profileInfo, rows.err = rows.ch.profileInfo(); rows.err != nil {
				return rows.err
			}
			rows.ch.logf("[rows] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case protocol.ServerData, protocol.ServerTotals, protocol.ServerExtremes:
			var (
				block *data.Block
				begin = time.Now()
			)
			if block, rows.err = rows.ch.readBlock(); rows.err != nil {
				return rows.err
			}
			rows.ch.logf("[rows] <- data: packet=%d, columns=%d, rows=%d, elapsed=%s", packet, block.NumColumns, block.NumRows, time.Since(begin))
			switch packet {
			case protocol.ServerData:
				for _, row := range convertBlockToDriverValues(block) {
					rows.stream <- row
				}
			case protocol.ServerTotals:
				rows.totals = convertBlockToDriverValues(block)
			case protocol.ServerExtremes:
				rows.extremes = convertBlockToDriverValues(block)
			}
			block.Reset()
		case protocol.ServerEndOfStream:
			rows.ch.logf("[rows] <- end of stream")
			return nil
		default:
			rows.ch.conn.Close()
			rows.ch.logf("[rows] unexpected packet [%d]", packet)
			return fmt.Errorf("[rows] unexpected packet [%d] from server", packet)
		}
	}
}

func (rows *rows) Close() error {
	rows.ch.logf("[rows] close")
	rows.columns = nil
	for !atomic.CompareAndSwapInt32(&rows.allDataIsReceived, 1, 2) {
		time.Sleep(time.Millisecond * 2)
	}
	if rows.finish != nil {
		rows.finish()
	}
	return nil
}

func convertBlockToDriverValues(block *data.Block) [][]driver.Value {
	values := make([][]driver.Value, 0, int(block.NumRows))
	for rowNum := 0; rowNum < int(block.NumRows); rowNum++ {
		row := make([]driver.Value, 0, block.NumColumns)
		for columnNum := 0; columnNum < int(block.NumColumns); columnNum++ {
			row = append(row, block.Values[columnNum][rowNum])
		}
		values = append(values, row)
	}
	return values
}
