package clickhouse

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
	if p.rows, err = readUvarint(ch.conn); err != nil {
		return nil, err
	}
	if p.bytes, err = readUvarint(ch.conn); err != nil {
		return nil, err
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
		if p.totalRows, err = readUvarint(ch.conn); err != nil {
			return nil, err
		}
	}
	return &p, nil
}
