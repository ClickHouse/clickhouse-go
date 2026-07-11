package proto

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckMinVersion pins the boundary semantics of the version gate used
// for the unsupported-server warning: a version passes only if it is equal
// to or greater than the constraint under major.minor.patch ordering.
func TestCheckMinVersion(t *testing.T) {
	constraint := Version{Major: 25, Minor: 8, Patch: 0}
	cases := []struct {
		version Version
		want    bool
	}{
		{Version{Major: 24, Minor: 12, Patch: 9}, false}, // major below, minor above
		{Version{Major: 25, Minor: 7, Patch: 9}, false},  // minor below, patch above
		{Version{Major: 25, Minor: 8, Patch: 0}, true},   // exact match
		{Version{Major: 25, Minor: 8, Patch: 1}, true},   // patch above
		{Version{Major: 25, Minor: 9, Patch: 0}, true},   // minor above
		{Version{Major: 26, Minor: 0, Patch: 0}, true},   // major above, minor below
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s vs %s", tc.version, constraint), func(t *testing.T) {
			assert.Equal(t, tc.want, CheckMinVersion(constraint, tc.version))
		})
	}

	// patch is the deciding field only when major and minor match exactly
	patchConstraint := Version{Major: 25, Minor: 8, Patch: 3}
	assert.False(t, CheckMinVersion(patchConstraint, Version{Major: 25, Minor: 8, Patch: 2}))
	assert.True(t, CheckMinVersion(patchConstraint, Version{Major: 25, Minor: 9, Patch: 0}))
}
