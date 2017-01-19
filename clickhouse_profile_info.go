package clickhouse

type profileInfo struct {
	rows                      uint64
	bytes                     uint64
	blocks                    uint64
	appliedLimit              bool
	rowsBeforeLimit           uint64
	calculatedRowsBeforeLimit bool
}

func (ch *clickhouse) profileInfo() (*profileInfo, error) {
	var (
		p   profileInfo
		err error
	)
	if p.rows, err = readUvariant(ch.conn); err != nil {
		return nil, err
	}
	if p.bytes, err = readUvariant(ch.conn); err != nil {
		return nil, err
	}
	if p.blocks, err = readUvariant(ch.conn); err != nil {
		return nil, err
	}
	if p.appliedLimit, err = readBool(ch.conn); err != nil {
		return nil, err
	}
	if p.rowsBeforeLimit, err = readUvariant(ch.conn); err != nil {
		return nil, err
	}
	if p.calculatedRowsBeforeLimit, err = readBool(ch.conn); err != nil {
		return nil, err
	}
	return &p, nil
}
