package contributors

import (
	_ "embed"
	"strings"
)

//go:embed list
var source string

var List []string = strings.Split(source, "\n")
