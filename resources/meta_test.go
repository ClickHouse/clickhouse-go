package resources

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestMinSupportedVersion(t *testing.T) {
	assert.Equal(t, proto.Version{Major: 25, Minor: 8, Patch: 0}, MinSupportedVersion)
}
