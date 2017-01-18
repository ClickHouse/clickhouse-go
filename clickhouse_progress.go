package clickhouse

type progress struct {
	rows      uint
	bytes     uint
	totalRows uint
}

func (ch *clickhouse) progress() (*progress, error) {
	var (
		p   progress
		err error
	)
	if p.rows, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if p.bytes, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
		if p.totalRows, err = ch.conn.readUInt(); err != nil {
			return nil, err
		}
	}
	return &p, nil
}
