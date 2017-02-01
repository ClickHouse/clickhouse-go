package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Parse_Enum(t *testing.T) {
	{
		if enum, err := parseEnum("Enum8('a' = 2, 'b' = 1)"); assert.NoError(t, err) {
			if value, err := enum.toValue("b"); assert.NoError(t, err) {
				if v, ok := value.(int8); assert.True(t, ok) {
					if assert.Equal(t, int8(1), v) {
						if _, err := enum.toValue("z"); !assert.Error(t, err) {
							return
						}
					}
				}
			}
			if value, err := enum.toValue("a"); assert.NoError(t, err) {
				if v, ok := value.(int8); assert.True(t, ok) {
					assert.Equal(t, int8(2), v)
				}
			}
		}
		if enum, err := parseEnum("Enum8('a' = 2, 'b' = 1)"); assert.NoError(t, err) {
			if ident, err := enum.toIdent(int8(1)); assert.NoError(t, err) {
				assert.Equal(t, "b", ident)
			}
			if ident, err := enum.toIdent(int8(2)); assert.NoError(t, err) {
				if assert.Equal(t, "a", ident) {
					if _, err := enum.toIdent(int8(100)); !assert.Error(t, err) {
						return
					}
				}
			}
		}
	}
	{
		if enum, err := parseEnum("Enum16('a' = 2, 'b' = 1)"); assert.NoError(t, err) {
			if value, err := enum.toValue("b"); assert.NoError(t, err) {
				if v, ok := value.(int16); assert.True(t, ok) {
					if assert.Equal(t, int16(1), v) {
						if _, err := enum.toValue("z"); !assert.Error(t, err) {
							return
						}
					}
				}
			}
			if value, err := enum.toValue("a"); assert.NoError(t, err) {
				if v, ok := value.(int16); assert.True(t, ok) {
					assert.Equal(t, int16(2), v)
				}
			}
		}
		if enum, err := parseEnum("Enum16('a' = 2, 'b' = 1)"); assert.NoError(t, err) {
			if ident, err := enum.toIdent(int16(1)); assert.NoError(t, err) {
				assert.Equal(t, "b", ident)
			}
			if ident, err := enum.toIdent(int16(2)); assert.NoError(t, err) {
				if assert.Equal(t, "a", ident) {
					if _, err := enum.toIdent(int16(100)); !assert.Error(t, err) {
						return
					}
				}
			}
		}
	}
}
