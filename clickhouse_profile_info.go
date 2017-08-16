package clickhouse

import "github.com/kshvakov/clickhouse/lib/binary"

type profileInfo struct {
	rows                      uint64
	bytes                     uint64
	blocks                    uint64
	appliedLimit              bool
	rowsBeforeLimit           uint64
	calculatedRowsBeforeLimit bool
}

func (ch *clickhouse) profileInfo(decoder *binary.Decoder) (*profileInfo, error) {
	var (
		p   profileInfo
		err error
	)
	if p.rows, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.blocks, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.bytes, err = decoder.Uvarint(); err != nil {
		return nil, err
	}

	if p.appliedLimit, err = decoder.Bool(); err != nil {
		return nil, err
	}
	if p.rowsBeforeLimit, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.calculatedRowsBeforeLimit, err = decoder.Bool(); err != nil {
		return nil, err
	}
	return &p, nil
}
