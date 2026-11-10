package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTime64ParseMalformed(t *testing.T) {
	sc := &ServerContext{Timezone: time.UTC}
	tests := []struct {
		name   string
		chType Type
	}{
		{name: "empty timezone", chType: "DateTime64(0,)"},
		{name: "unquoted timezone", chType: "DateTime64(0,UTC)"},
		{name: "single quote", chType: "DateTime64(0,')"},
		{name: "empty precision", chType: "DateTime64(, 'UTC')"},
		{name: "precision not a number", chType: "DateTime64(abc, 'UTC')"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				col, err := tt.chType.Column("col", sc)
				require.Error(t, err)
				assert.Nil(t, col)
			})
		})
	}
}

func TestDateTime64ParseValid(t *testing.T) {
	sc := &ServerContext{Timezone: time.UTC}
	tests := []struct {
		name     string
		chType   Type
		wantZone *time.Location
	}{
		{name: "timezone with space", chType: "DateTime64(3, 'UTC')", wantZone: time.UTC},
		{name: "timezone without space", chType: "DateTime64(3,'UTC')", wantZone: time.UTC},
		// an empty quoted name resolves to UTC, like time.LoadLocation("")
		{name: "empty timezone name", chType: "DateTime64(3, '')", wantZone: time.UTC},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			col, err := tt.chType.Column("col", sc)
			require.NoError(t, err)
			require.IsType(t, &DateTime64{}, col)
			assert.Equal(t, tt.chType, col.Type())
			assert.Equal(t, tt.wantZone, col.(*DateTime64).col.Location)
		})
	}
}
