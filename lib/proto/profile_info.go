package proto

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type ProfileInfo struct {
	Rows                      uint64
	Bytes                     uint64
	Blocks                    uint64
	AppliedLimit              bool
	RowsBeforeLimit           uint64
	CalculatedRowsBeforeLimit bool
}

func (p *ProfileInfo) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if p.Rows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.Blocks, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.Bytes, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.AppliedLimit, err = decoder.Bool(); err != nil {
		return err
	}
	if p.RowsBeforeLimit, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.CalculatedRowsBeforeLimit, err = decoder.Bool(); err != nil {
		return err
	}
	return nil
}

func (p *ProfileInfo) String() string {
	return fmt.Sprintf("rows=%d, bytes=%d, blocks=%d, rows before limit=%d, applied limit=%t, calculated rows before limit=%t",
		p.Rows,
		p.Bytes,
		p.Blocks,
		p.RowsBeforeLimit,
		p.AppliedLimit,
		p.CalculatedRowsBeforeLimit,
	)
}
