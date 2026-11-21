package kismockread

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

type staticGetter struct {
	value   string
	present bool
	err     error
	calls   int
	key     string
}

func (getter *staticGetter) Get(_ context.Context, key string) (string, bool, error) {
	getter.calls++
	getter.key = key
	return getter.value, getter.present, getter.err
}

type recordingTransport struct {
	calls    int
	requests []*http.Request
	respond  func(*http.Request) (*http.Response, error)
}

func (transport *recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls++
	transport.requests = append(transport.requests, request)
	return transport.respond(request)
}

func testConfig() Config {
	return Config{
		BaseURL:   MockBaseURL,
		AppKey:    "app-key-for-test",
		AppSecret: "app-secret-for-test",
		AccountNo: "12345678-01",
		Timeout:   time.Second,
	}
}

func testNow() time.Time {
	return time.Unix(2_000_000_000, 0)
}

func redactionProbeToken() string {
	return strings.Join([]string{
		"eyJhbGciOiJIUzI1NiJ9",
		"redaction_probe_7f3a",
		"signature_44e9",
	}, ".")
}

func cachedTokenPayload(token string, expiresAt float64) string {
	payload, err := json.Marshal(map[string]any{
		"access_token": token,
		"expires_at":   expiresAt,
	})
	if err != nil {
		panic(err)
	}
	return string(payload)
}

func jsonResponse(status int, payload string, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(payload)),
	}
}
