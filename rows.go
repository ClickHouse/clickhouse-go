package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	"github.com/kshvakov/clickhouse/internal/binary"
	"github.com/kshvakov/clickhouse/internal/data"
	"github.com/kshvakov/clickhouse/internal/protocol"
)

type rows struct {
	err      error
	ch       *clickhouse
	block    *data.Block
	stream   chan []driver.Value
	columns  []string
	totals   [][]driver.Value
	extremes [][]driver.Value
}

func (rows *rows) Columns() []string {
	return rows.columns
}

func (rows *rows) ColumnTypeScanType(idx int) reflect.Type {
	return rows.block.Columns[idx].ScanType()
}

func (rows *rows) ColumnTypeDatabaseTypeName(idx int) string {
	return rows.block.Columns[idx].CHType()
}

func (rows *rows) Next(dest []driver.Value) error {
	row, open := <-rows.stream
	if !open && row == nil {
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

func (rows *rows) receiveData() {
	var (
		packet   uint64
		block    *data.Block
		progress *progress
		decoder  = binary.NewDecoder(rows.ch.conn)
	)
	defer close(rows.stream)
	for {
		if packet, rows.err = decoder.Uvarint(); rows.err != nil {
			return
		}
		switch packet {
		case protocol.ServerException:
			rows.ch.logf("[receive data] <- exception")
			rows.err = rows.ch.exception()
			return
		case protocol.ServerProgress:
			progress, rows.err = rows.ch.progress()
			if rows.err != nil {
				return
			}
			rows.ch.logf("[receive data] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.rows,
				progress.bytes,
				progress.totalRows,
			)
		case protocol.ServerProfileInfo:
			profileInfo, err := rows.ch.profileInfo()
			if err != nil {
				return
			}
			rows.ch.logf("[receive data] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)

		case protocol.ServerData, protocol.ServerTotals, protocol.ServerExtremes:
			if block, rows.err = rows.ch.readBlock(decoder); rows.err != nil {
				return
			}
			if len(rows.columns) == 0 && len(block.Columns) != 0 {
				rows.columns = block.ColumnNames()
			}
			rows.ch.logf("[receive data] <- data: packet=%d, columns=%d, rows=%d", packet, block.NumColumns, block.NumRows)
			values := convertBlockToDriverValues(block)
			switch block.Reset(); packet {
			case protocol.ServerData:
				for _, value := range values {
					if len(value) != 0 {
						rows.stream <- value
					}
				}
			case protocol.ServerTotals:
				rows.totals = values
			case protocol.ServerExtremes:
				rows.extremes = values
			}
		case protocol.ServerEndOfStream:
			rows.ch.logf("[receive data] <- end of stream")
			return
		default:
			rows.ch.logf("[receive data] unexpected packet [%d]", packet)
			rows.err = fmt.Errorf("unexpected packet [%d] from server", packet)
			return
		}
	}
}

func (rows *rows) Close() error {
	rows.totals = nil
	rows.columns = nil
	rows.extremes = nil
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
