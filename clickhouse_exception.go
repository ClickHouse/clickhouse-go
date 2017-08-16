package clickhouse

import (
	"fmt"
	"strings"

	"github.com/kshvakov/clickhouse/lib/binary"
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

func (ch *clickhouse) exception(decoder *binary.Decoder) error {
	var (
		e         Exception
		err       error
		hasNested bool
	)
	if e.Code, err = decoder.Int32(); err != nil {
		return err
	}
	if e.Name, err = decoder.String(); err != nil {
		return err
	}
	if e.Message, err = decoder.String(); err != nil {
		return err
	}
	e.Message = strings.TrimSpace(strings.TrimPrefix(e.Message, e.Name+":"))
	if e.StackTrace, err = decoder.String(); err != nil {
		return err
	}
	if hasNested, err = decoder.Bool(); err != nil {
		return err
	}
	if hasNested {
		e.nested = ch.exception(decoder)
	}
	return &e
}
