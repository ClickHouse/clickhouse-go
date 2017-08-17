package clickhouse

import "github.com/kshvakov/clickhouse/lib/protocol"

type progress struct {
	rows      uint64
	bytes     uint64
	totalRows uint64
}

func (ch *clickhouse) progress() (*progress, error) {
	var (
		p   progress
		err error
	)
	if p.rows, err = ch.decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.bytes, err = ch.decoder.Uvarint(); err != nil {
		return nil, err
	}
	if ch.ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
		if p.totalRows, err = ch.decoder.Uvarint(); err != nil {
			return nil, err
		}
	}
	return &p, nil
}
