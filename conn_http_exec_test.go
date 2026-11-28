package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSetStatement(t *testing.T) {
	testCases := []struct {
		query string
		want  bool
	}{
		{query: "SET readonly = 1", want: true},
		{query: "  set readonly = 1", want: true},
		{query: "\n\tSET\treadonly = 1", want: true},
		{query: "-- comment\nSET readonly = 1", want: true},
		{query: "# comment\nSET readonly = 1", want: true},
		{query: "/* comment */ SET readonly = 1", want: true},
		{query: "/* a */ -- b\n /* c */SET readonly = 1", want: true},
		{query: "SETTINGS", want: false},
		{query: "SET", want: false},
		{query: "SELECT 1", want: false},
		{query: "-- SET readonly = 1\nSELECT 1", want: false},
		{query: "/* SET readonly = 1 */ SELECT 1", want: false},
		{query: "/* unterminated SET readonly = 1", want: false},
		{query: "INSERT INTO t SETTINGS async_insert = 1 VALUES (1)", want: false},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.want, isSetStatement(tc.query), tc.query)
	}
}

// The readonly read after a SET keeps the authentication and the session of the SET.
func TestReadOnlyQueryOptions(t *testing.T) {
	ctx := Context(context.Background(),
		WithJWT("token"),
		WithQuotaKey("quota"),
		WithQueryID("set-query"),
		WithParameters(Parameters{"p": "1"}),
		WithSettings(Settings{"session_id": "s1", "max_threads": 1}),
	)

	opt := readOnlyQueryOptions(ctx)
	assert.Equal(t, "token", opt.jwt)
	assert.Equal(t, "quota", opt.quotaKey)
	assert.Equal(t, Settings{"session_id": "s1"}, opt.settings)
	assert.Empty(t, opt.queryID)
	assert.Nil(t, opt.parameters)

	// The options of the SET are not changed.
	setOpt := queryOptions(ctx)
	assert.Equal(t, "set-query", setOpt.queryID)
	assert.Len(t, setOpt.settings, 2)

	assert.Equal(t, Settings{}, readOnlyQueryOptions(context.Background()).settings)
}
