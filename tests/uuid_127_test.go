//go:build go1.27

package tests

import (
	"testing"
	"uuid"
)

func TestUUIDStd(t *testing.T) {
	ids := [1000]uuid.UUID{}
	for i := range ids {
		ids[i] = uuid.New()
	}
	testUUIDImplementation(t, ids)
}
