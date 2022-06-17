package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
)

// Begin starts and returns a new transaction.
func (c *httpConnOpener) Begin() (driver.Tx, error) {
	return c, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *httpConnOpener) Prepare(query string) (driver.Stmt, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	query = splitInsertRe.Split(query, -1)[0]
	if !strings.HasSuffix(strings.TrimSpace(strings.ToUpper(query)), "VALUES") {
		query += " VALUES "
	}

	batch := newHttpBatch(c, query, c.location)
	c.stmts = append(c.stmts, batch)

	return batch, nil
}

// Commit applies prepared statement if it exists
func (c *httpConnOpener) Commit() (err error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return driver.ErrBadConn
	}

	stmts := c.stmts
	c.stmts = stmts[:0]

	if len(stmts) == 0 {
		return nil
	}

	for _, stmt := range stmts {
		if err = stmt.commit(); err != nil {
			break
		}
	}

	return
}

// Rollback cleans prepared statement
func (c *httpConnOpener) Rollback() error {
	if atomic.LoadInt32(&c.closed) != 0 {
		return driver.ErrBadConn
	}

	if len(c.stmts) == 0 {
		return sql.ErrTxDone
	}

	stmts := c.stmts
	c.stmts = stmts[:0]

	return nil
}
