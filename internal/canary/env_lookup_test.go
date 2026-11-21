package canary

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Regression: cmd/edge-canary passes Options{} (nil Lookup). Run must then read
// the process environment; otherwise CANARY_EDGE_URL is silently ignored and
// the canary targets the 8080 default with the 1000 KRW default price.
func TestRunWithNilLookupReadsProcessEnvironment(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	t.Setenv("CANARY_EDGE_URL", server.URL)
	t.Setenv("CANARY_TEXTFILE_DIR", t.TempDir())
	// 10:00 KST on a weekday sits inside the KRX canary window.
	inSession := time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)
	var stdout, stderr bytes.Buffer
	Run(context.Background(), Options{Now: func() time.Time { return inSession }, Stdout: &stdout, Stderr: &stderr})

	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("expected exactly one placement request at CANARY_EDGE_URL, got %d (stdout=%s stderr=%s)", hits, stdout.String(), stderr.String())
	}
}
