package clickhouse

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"

	"github.com/kshvakov/clickhouse/internal/binary"
	"github.com/kshvakov/clickhouse/internal/data"
	"github.com/kshvakov/clickhouse/internal/protocol"
)

var (
	ErrInsertInNotBatchMode = errors.New("insert statement supported only in the batch mode (use begin/commit)")
	ErrLimitDataRequestInTx = errors.New("data request has already been prepared in transaction")
)

var (
	splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)
)

type logger func(format string, v ...interface{})

type clickhouse struct {
	data.ServerInfo
	data.ClientInfo
	logf          logger
	conn          *connect
	block         *data.Block
	buffer        *bufio.ReadWriter
	decoder       *binary.Decoder
	encoder       *binary.Encoder
	compress      bool
	blockSize     int
	inTransaction bool
}

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	return ch.prepareContext(context.Background(), query)
}

func (ch *clickhouse) prepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if ch.conn.isBad {
		return nil, driver.ErrBadConn
	}
	ch.logf("[prepare] %s", query)
	if ch.block != nil {
		return nil, ErrLimitDataRequestInTx
	}
	if isInsert(query) {
		if !ch.inTransaction {
			return nil, ErrInsertInNotBatchMode
		}
		return ch.insert(query)
	}
	return &stmt{
		ch:       ch,
		query:    query,
		numInput: numInput(query),
	}, nil
}

func (ch *clickhouse) insert(query string) (driver.Stmt, error) {
	if err := ch.sendQuery(splitInsertRe.Split(query, -1)[0] + " VALUES "); err != nil {
		return nil, err
	}
	for {
		packet, err := ch.decoder.Uvarint()
		if err != nil {
			return nil, err
		}
		switch packet {
		case protocol.ServerData:
			if ch.block, err = ch.readBlock(); err != nil {
				return nil, err
			}
			return &stmt{
				ch:       ch,
				isInsert: true,
			}, nil
		case protocol.ServerException:
			return nil, ch.exception()
		default:
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	return ch.beginTx(context.Background(), txOptions{})
}

type txOptions struct {
	Isolation int
	ReadOnly  bool
}

func (ch *clickhouse) beginTx(ctx context.Context, opts txOptions) (driver.Tx, error) {
	ch.logf("[begin] tx=%t, data=%t, bad connection=%t", ch.inTransaction, ch.block != nil, ch.conn.isBad)
	switch {
	case ch.inTransaction:
		return nil, sql.ErrTxDone
	case ch.conn.isBad:
		return nil, driver.ErrBadConn
	}
	if finish := ch.watchCancel(ctx); finish != nil {
		defer finish()
	}
	ch.block = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Commit() error {
	ch.logf("[commit] tx=%t, data=%t, bad connection=%t", ch.inTransaction, ch.block != nil, ch.conn.isBad)
	switch {
	case !ch.inTransaction:
		return sql.ErrTxDone
	case ch.conn.isBad:
		return driver.ErrBadConn
	}
	defer func() {
		ch.block.Reset()
		ch.block = nil
		ch.inTransaction = false
	}()
	if ch.block != nil {
		if err := ch.writeBlock(ch.block); err != nil {
			return err
		}
	}
	// Send empty block as marker of end of data.
	if err := ch.writeBlock(&data.Block{}); err != nil {
		return err
	}
	ch.buffer.Flush()
	return ch.wait()
}

func (ch *clickhouse) Rollback() error {
	ch.logf("[rollback] tx=%t, data=%t, bad connection=%t", ch.inTransaction, ch.block != nil, ch.conn.isBad)
	switch {
	case !ch.inTransaction:
		return sql.ErrTxDone
	case ch.conn.isBad:
		return driver.ErrBadConn
	}
	ch.block = nil
	ch.inTransaction = false
	return ch.conn.Close()
}

func (ch *clickhouse) Close() error {
	ch.block = nil
	return ch.conn.Close()
}

func (ch *clickhouse) wait() error {
	packet, err := ch.decoder.Uvarint()
	if err != nil {
		return err
	}
	for {
		switch packet {
		case protocol.ServerException:
			ch.logf("[got packet] <- exception")
			return ch.exception()
		case protocol.ServerProgress:
			progress, err := ch.progress()
			if err != nil {
				return err
			}
			ch.logf("[got packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case protocol.ServerEndOfStream:
			return nil
		default:
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
		if packet, err = ch.decoder.Uvarint(); err != nil {
			return err
		}
	}
}

func (ch *clickhouse) cancel() error {
	ch.logf("cancel request")
	if err := ch.encoder.Uvarint(protocol.ClientCancel); err != nil {
		return err
	}
	return nil //ch.conn.Close()
}

func (ch *clickhouse) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				ch.cancel()
				finished <- struct{}{}
				ch.logf("[ch] watchCancel <- done")
			case <-finished:
				ch.logf("[ch] watchCancel <- finished")
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}
