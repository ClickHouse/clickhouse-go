package proto

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Exception struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	Nested     []Exception
	nested     bool
}

func (e *Exception) Error() string {
	return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
}

func (e *Exception) Decode(decoder *binary.Decoder) (err error) {
	var exceptions []Exception
	for {
		var ex Exception
		if err := ex.decode(decoder); err != nil {
			return err
		}
		if exceptions = append(exceptions, ex); !ex.nested {
			break
		}
	}
	if len(exceptions) != 0 {
		e.Code = exceptions[0].Code
		e.Name = exceptions[0].Name
		e.Message = exceptions[0].Message
		e.StackTrace = exceptions[0].StackTrace
		if exceptions[0].nested {
			e.Nested = exceptions[1:]
		}
	}
	return nil
}

func (e *Exception) decode(decoder *binary.Decoder) (err error) {
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
	if e.nested, err = decoder.Bool(); err != nil {
		return err
	}
	return nil
}
