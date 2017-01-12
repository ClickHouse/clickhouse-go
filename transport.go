package clickhouse

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"sync/atomic"
)

var tick int32

type transport struct {
	origin   http.RoundTripper
	scheme   string
	hosts    []string
	username string
	password string
}

func (t *transport) RoundTrip(req *http.Request) (response *http.Response, err error) {
	req.URL.Scheme = t.scheme
	if len(t.username) != 0 && len(t.password) != 0 {
		req.SetBasicAuth(t.username, t.password)
	}
	var (
		body, _ = ioutil.ReadAll(req.Body)
		index   = abs(int(atomic.AddInt32(&tick, 1)))
	)
	for i := 0; i <= len(t.hosts); i++ {
		req.URL.Host = t.hosts[(index+i)%len(t.hosts)]
		req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		if response, err = t.origin.RoundTrip(req); err == nil && (response.StatusCode == http.StatusOK || response.StatusCode == http.StatusInternalServerError) {
			return
		}
	}
	return
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}
