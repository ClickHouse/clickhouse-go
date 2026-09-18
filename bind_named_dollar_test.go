package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBindNamedDollar(t *testing.T) {
	for _, name := range []string{"id$x", "$x", "id$", "a$b", "$", "a$$b", "id$1"} {
		t.Run(name, func(t *testing.T) {
			query := "SELECT @" + name + ", @id"
			got, err := bind(time.UTC, query, Named("id", 7), Named(name, 42))
			require.NoError(t, err)
			require.Equal(t, "SELECT 42, 7", got)

			_, err = bind(time.UTC, "SELECT @"+name, Named("id", 7))
			require.ErrorContains(t, err, "@"+name)
		})
	}
	t.Run("quoted_and_commented_names", func(t *testing.T) {
		query := "SELECT '@id$x', \"@id$x\", `@id$x`, @id$x /* @id$x */ -- @id$x\n"
		got, err := bind(time.UTC, query, Named("id$x", 42))
		require.NoError(t, err)
		require.Equal(t, "SELECT '@id$x', \"@id$x\", `@id$x`, 42 /* @id$x */ -- @id$x\n", got)
	})
	t.Run("existing_missing_names", func(t *testing.T) {
		for _, name := range []string{"id2", "id_x"} {
			_, err := bind(time.UTC, "SELECT @"+name, Named("id", 7))
			require.ErrorContains(t, err, "@"+name)
		}
	})
	t.Run("date_named", func(t *testing.T) {
		value := time.Date(2026, 1, 2, 3, 4, 5, 123000000, time.UTC)
		want, err := bind(time.UTC, "SELECT @date", DateNamed("date", value, MilliSeconds))
		require.NoError(t, err)
		got, err := bind(time.UTC, "SELECT @date$value", DateNamed("date$value", value, MilliSeconds))
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("other_placeholder_formats", func(t *testing.T) {
		for _, query := range []string{"SELECT ?", "SELECT $1"} {
			got, err := bind(time.UTC, query, 42)
			require.NoError(t, err)
			require.Equal(t, "SELECT 42", got)
		}
	})
}
