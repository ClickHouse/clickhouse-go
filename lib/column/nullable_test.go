package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNullableAppend(t *testing.T) {
	null := Nullable{
		base: &String{},
	}
	var (
		a = "a"
		b = "b"
	)
	values := []*string{
		&a,
		nil,
		&b,
		nil,
		nil,
	}
	if nulls, err := null.Append(values); assert.NoError(t, err) {
		assert.Equal(t, &String{"a", "", "b", "", ""}, null.base)
		assert.Equal(t, UInt8{0, 1, 0, 1, 1}, null.nulls)
		t.Log(nulls)
	}
}
