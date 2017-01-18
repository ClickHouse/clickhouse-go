package clickhouse

type profileInfo struct {
	rows                      uint
	bytes                     uint
	blocks                    uint
	appliedLimit              bool
	rowsBeforeLimit           uint
	calculatedRowsBeforeLimit bool
}

func (ch *clickhouse) profileInfo() (*profileInfo, error) {
	var (
		p   profileInfo
		err error
	)
	if p.rows, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if p.bytes, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if p.blocks, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if p.appliedLimit, err = ch.conn.readBinaryBool(); err != nil {
		return nil, err
	}
	if p.rowsBeforeLimit, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if p.calculatedRowsBeforeLimit, err = ch.conn.readBinaryBool(); err != nil {
		return nil, err
	}
	return &p, nil
}
