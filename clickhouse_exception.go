package clickhouse

import (
	"fmt"
	"strings"
)

type Exception struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	nested     error
}

func (e *Exception) Error() string {
	return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
}

func (ch *clickhouse) exception() error {
	var (
		e         Exception
		err       error
		hasNested bool
	)
	if e.Code, err = ch.conn.readBinaryInt32(); err != nil {
		return err
	}
	if e.Name, err = ch.conn.readString(); err != nil {
		return err
	}
	if e.Message, err = ch.conn.readString(); err != nil {
		return err
	}
	e.Message = strings.TrimSpace(strings.TrimPrefix(e.Message, e.Name+":"))
	if e.StackTrace, err = ch.conn.readString(); err != nil {
		return err
	}
	if hasNested, err = ch.conn.readBinaryBool(); err != nil {
		return err
	}
	if hasNested {
		e.nested = ch.exception()
	}
	return &e
}
