package proto

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Progress struct {
	Rows       uint64
	Bytes      uint64
	TotalRows  uint64
	WroteRows  uint64
	WroteBytes uint64
	withClient bool
}

func (p *Progress) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if p.Rows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.Bytes, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.TotalRows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
		p.withClient = true
		if p.WroteRows, err = decoder.Uvarint(); err != nil {
			return err
		}
		if p.WroteBytes, err = decoder.Uvarint(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Progress) String() string {
	if !p.withClient {
		return fmt.Sprintf("rows=%d, bytes=%d, total rows=%d", p.Rows, p.Bytes, p.TotalRows)
	}
	return fmt.Sprintf("rows=%d, bytes=%d, total rows=%d, wrote rows=%d wrote bytes=%d",
		p.Rows,
		p.Bytes,
		p.TotalRows,
		p.WroteRows,
		p.WroteBytes,
	)
}
