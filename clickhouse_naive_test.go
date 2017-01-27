package clickhouse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Naive_Result(t *testing.T) {
	var result result
	if _, err := result.LastInsertId(); assert.Error(t, err) {
		if rows, err := result.RowsAffected(); assert.Error(t, err) {
			assert.Equal(t, int64(0), rows)
		}
	}
}

func Test_Naive_Exception(t *testing.T) {
	exception := Exception{
		Code:    42,
		Message: "test",
	}
	if assert.Implements(t, (*error)(nil), &exception) {
		assert.Equal(t, fmt.Sprintf("code: %d, message: %s", exception.Code, exception.Message), exception.Error())
	}
}
