package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/kshvakov/clickhouse/lib/column"
	"github.com/kshvakov/clickhouse/lib/protocol"
)

type rows struct {
	ch                *clickhouse
	index             int
	finish            func()
	values            [][]interface{}
	totals            [][]interface{}
	extremes          [][]interface{}
	columns           []string
	blockColumns      []column.Column
	allDataIsReceived bool
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
	for len(rows.values) == 0 || len(rows.values[0]) <= rows.index {
		if rows.allDataIsReceived {
			return io.EOF
		}
		if err := rows.receiveData(); err != nil {
			return err
		}
	}
	for i := range dest {
		dest[i] = rows.values[i][rows.index]
	}
	rows.index++
	if len(rows.values) == 0 || len(rows.values[0]) <= rows.index {
		rows.values = nil
		for !(rows.allDataIsReceived || len(rows.values) != 0) {
			if err := rows.receiveData(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rows *rows) HasNextResultSet() bool {
	return len(rows.totals) != 0 || len(rows.extremes) != 0
}

func (rows *rows) NextResultSet() error {
	switch {
	case len(rows.totals) != 0:
		for _, value := range rows.totals {
			rows.values = append(rows.values, value)
		}
		rows.index = 0
		rows.totals = nil
	case len(rows.extremes) != 0:
		for _, value := range rows.extremes {
			rows.values = append(rows.values, value)
		}
		rows.index = 0
		rows.extremes = nil
	default:
		return io.EOF
	}
	return nil
}

func (rows *rows) receiveData() error {
	for {
		packet, err := rows.ch.decoder.Uvarint()
		if err != nil {
			return err
		}
		switch packet {
		case protocol.ServerException:
			rows.ch.logf("[rows] <- exception")
			return rows.ch.exception()
		case protocol.ServerProgress:
			progress, err := rows.ch.progress()
			if err != nil {
				return err
			}
			rows.ch.logf("[rows] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.rows,
				progress.bytes,
				progress.totalRows,
			)
		case protocol.ServerProfileInfo:
			profileInfo, err := rows.ch.profileInfo()
			if err != nil {
				return err
			}
			rows.ch.logf("[rows] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case protocol.ServerData, protocol.ServerTotals, protocol.ServerExtremes:
			var (
				begin      = time.Now()
				block, err = rows.ch.readBlock()
			)
			if err != nil {
				return err
			}
			rows.ch.logf("[rows] <- data: packet=%d, columns=%d, rows=%d, elapsed=%s", packet, block.NumColumns, block.NumRows, time.Since(begin))
			if len(rows.columns) == 0 && len(block.Columns) != 0 {
				rows.columns = block.ColumnNames()
				rows.blockColumns = block.Columns
				if block.NumRows == 0 {
					return nil
				}
			}
			switch block.Reset(); packet {
			case protocol.ServerData:
				rows.index = 0
				rows.values = block.Values
			case protocol.ServerTotals:
				rows.totals = block.Values
			case protocol.ServerExtremes:
				rows.extremes = block.Values
			}
			if len(rows.values) != 0 {
				return nil
			}
		case protocol.ServerEndOfStream:
			rows.allDataIsReceived = true
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
	if rows.finish != nil {
		rows.finish()
	}
	return nil
}
