package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/kshvakov/clickhouse/internal/data"
	"github.com/kshvakov/clickhouse/internal/protocol"
)

type rows struct {
	err      error
	ch       *clickhouse
	stream   chan []driver.Value
	columns  []string
	totals   [][]driver.Value
	extremes [][]driver.Value
}

func (rows *rows) Columns() []string {
	return rows.columns
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
		packet uint64
		block  *data.Block
	)
	defer close(rows.stream)
	for {
		if packet, rows.err = rows.ch.decoder.Uvarint(); rows.err != nil {
			return
		}
		switch packet {
		case protocol.ServerException:
			rows.err = rows.ch.exception()
			return
		case protocol.ServerEndOfStream:
			return
		case protocol.ServerProfileInfo:
			profileInfo, err := rows.ch.profileInfo()
			if err != nil {
				return
			}
			rows.ch.logf("[receive packet] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case protocol.ServerProgress:
			progress, err := rows.ch.progress()
			if err != nil {
				return
			}
			rows.ch.logf("[receive packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case protocol.ServerData, protocol.ServerTotals, protocol.ServerExtremes:
			if block, rows.err = rows.ch.readBlock(); rows.err != nil {
				return
			}
			if len(rows.columns) == 0 && len(block.Columns) != 0 {
				rows.columns = block.ColumnNames()
			}
			values := convertBlockToDriverValues(block)
			switch packet {
			case protocol.ServerData:
				for _, value := range values {
					if len(value) != 0 {
						rows.stream <- value
					}
				}
			case protocol.ServerTotals:
			}
			block.Reset()
		default:
			fmt.Println("PPPPPPPPP", packet)
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
