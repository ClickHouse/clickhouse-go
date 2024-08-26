package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractEnumNamedValues(t *testing.T) {
	tests := []struct {
		name           string
		chType         Type
		expectedType   string
		expectedValues map[int]string
		isNotValid     bool
	}{
		{
			name:         "Enum8",
			chType:       "Enum8('a'=1,'b'=2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum16",
			chType:       "Enum16('a'=1,'b'=2)",
			expectedType: "Enum16",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with comma in value",
			chType:       "Enum8('a'=1,'b'=2,'c,d'=3)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
				3: "c,d",
			},
		},
		{
			name:         "Enum8 with spaces",
			chType:       "Enum8('a' = 1, 'b' = 2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 without indexes",
			chType:       "Enum8('a','b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with a first index only",
			chType:       "Enum8('a'=1,'b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with a last index only",
			chType:       "Enum8('a','b'=5)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				5: "b",
			},
		},
		{
			name:         "Enum8 with a first index only higher than 1",
			chType:       "Enum8('a'=5,'b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				5: "a",
				6: "b",
			},
		},
		{
			name:         "Enum8 with index with spaces",
			chType:       "Enum8( 'a' , 'b' = 5 )",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				5: "b",
			},
		},
		{
			name:         "Enum8 with escaped quotes",
			chType:       `Enum8('a\'b'=1)`,
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a'b",
			},
		},
		{
			name:       "Enum8 with invalid index",
			chType:     "Enum8('a'=1,'b'=256)",
			isNotValid: true,
		},
		{
			name:       "Enum8 with invalid non-integer index",
			chType:     "Enum8('a'=1,'b'='c')",
			isNotValid: true,
		},
		{
			name:       "Empty Enum8",
			chType:     "Enum8()",
			isNotValid: true,
		},
		{
			name:       "Empty Enum8 without brackets",
			chType:     "Enum8",
			isNotValid: true,
		},
		{
			name:         "Enum8 with empty key",
			chType:       "Enum8('a'=1, ''=2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualType, actualValues, actualIndexes, valid := extractEnumNamedValues(tt.chType)

			if tt.isNotValid {
				assert.False(t, valid, "%s is valid enum", tt.chType)
				return
			}

			actualValuesMap := make(map[int]string)
			for i, v := range actualValues {
				actualValuesMap[actualIndexes[i]] = v
			}

			assert.Equal(t, tt.expectedType, actualType)
			assert.Equal(t, tt.expectedValues, actualValuesMap)

			assert.True(t, valid, "%s is not valid enum", tt.chType)
		})
	}
}
