package clickhouse

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientInfoAppend(t *testing.T) {
	a := ClientInfo{
		Products: []struct {
			Name    string
			Version string
		}{
			{
				Name:    "product",
				Version: "1.0.0",
			},
		},
		Comment: []string{"comment_a"},
	}

	b := ClientInfo{
		Products: []struct {
			Name    string
			Version string
		}{
			{
				Name:    "product2",
				Version: "2.0.0",
			},
		},
		Comment: []string{"comment_b"},
	}

	c := a.Append(b)

	// Check first ClientInfo unchanged
	require.Len(t, a.Products, 1)
	require.Equal(t, "product", a.Products[0].Name)
	require.Equal(t, "1.0.0", a.Products[0].Version)
	require.Len(t, a.Comment, 1)
	require.Equal(t, "comment_a", a.Comment[0])

	// Check second ClientInfo unchanged
	require.Len(t, b.Products, 1)
	require.Equal(t, "product2", b.Products[0].Name)
	require.Equal(t, "2.0.0", b.Products[0].Version)
	require.Len(t, b.Comment, 1)
	require.Equal(t, "comment_b", b.Comment[0])

	// Verify third ClientInfo is merged correctly
	require.Len(t, c.Products, 2)
	require.Equal(t, "product", c.Products[0].Name)
	require.Equal(t, "1.0.0", c.Products[0].Version)
	require.Equal(t, "product2", c.Products[1].Name)
	require.Equal(t, "2.0.0", c.Products[1].Version)

	require.Len(t, c.Comment, 2)
	require.Equal(t, "comment_a", c.Comment[0])
	require.Equal(t, "comment_b", c.Comment[1])

}

func TestClientInfoString(t *testing.T) {
	// e.g. clickhouse-go/2.5.1
	expectedClientProduct := fmt.Sprintf("%s/%d.%d.%d", ClientName, ClientVersionMajor, ClientVersionMinor, ClientVersionPatch)

	// e.g. lv:go/1.19.5; os:darwin
	expectedDefaultMeta := fmt.Sprintf("lv:go/%s; os:%s", runtime.Version()[2:], runtime.GOOS)

	testCases := map[string]struct {
		actual   ClientInfo
		expected string
	}{
		"client": {
			ClientInfo{},
			// e.g. clickhouse-go/2.5.1 (lv:go/1.19.5; os:darwin)
			fmt.Sprintf("%s (%s)", expectedClientProduct, expectedDefaultMeta),
		},
		"client with comment": {
			ClientInfo{
				Comment: []string{"database/sql"},
			},
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.5; os:darwin)
			fmt.Sprintf("%s (database/sql; %s)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional product": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
			},
			// e.g. grafana-datasource/0.1.1 clickhouse-go/2.5.1 (lv:go/1.19.5; os:darwin)
			fmt.Sprintf("grafana-datasource/0.1.1 %s (%s)", expectedClientProduct, expectedDefaultMeta),
		},
		"additional products with comment": {
			ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "grafana", Version: "6.1"},
					{Name: "grafana-datasource", Version: "0.1.1"},
				},
				Comment: []string{"database/sql"},
			},
			// e.g. grafana/6.1 grafana-datasource/0.1.1 clickhouse-go/2.5.1 (database/sql; lv:go/1.19.5; os:darwin)
			fmt.Sprintf("grafana/6.1 grafana-datasource/0.1.1 %s (database/sql; %s)", expectedClientProduct, expectedDefaultMeta),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.actual.String()

			assert.Equal(t, testCase.expected, actual)
		})
	}
}
