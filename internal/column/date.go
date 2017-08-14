package column

import (
	"fmt"
)

type Date struct {
	name, chType string
}

func (date *Date) Name() string {
	return date.name
}

func (date *Date) CHType() string {
	return date.chType
}

func (date *Date) String() string {
	return fmt.Sprintf("%s (%s)", date.name, date.chType)
}
