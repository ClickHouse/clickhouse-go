package chcol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVariantHasType(t *testing.T) {
	cases := []struct {
		name     string
		input    Variant
		expected bool
	}{
		{
			name:     "NewVariant has no type",
			input:    NewVariant(42),
			expected: false,
		},
		{
			name:     "NewVariantWithType has a type",
			input:    NewVariantWithType(42, "Int64"),
			expected: true,
		},
		{
			name:     "NewVariantWithType with a nil value still has a type",
			input:    NewVariantWithType(nil, "String"),
			expected: true,
		},
		{
			name:     "WithType adds a type to an untyped Variant",
			input:    NewVariant("hello").WithType("String"),
			expected: true,
		},
		{
			name:     "NewDynamic has no type",
			input:    NewDynamic(42),
			expected: false,
		},
		{
			name:     "NewDynamicWithType has a type",
			input:    NewDynamicWithType(42, "Int64"),
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.expected, c.input.HasType())
			require.Equal(t, c.expected, c.input.Type() != "", "HasType must agree with Type")
		})
	}
}
