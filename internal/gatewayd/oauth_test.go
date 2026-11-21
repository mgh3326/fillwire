package gatewayd

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

type recordingOAuthTransport struct {
	calls   int
	request *http.Request
	body    string
	respond func(*http.Request) *http.Response
}

func (transport *recordingOAuthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls++
	transport.request = request
	body, _ := io.ReadAll(request.Body)
	transport.body = string(body)
	return transport.respond(request), nil
}

func TestKISMockOAuthClientUsesPinnedVTSPost(t *testing.T) {
	transport := &recordingOAuthTransport{
		respond: func(*http.Request) *http.Response {
			return oauthResponse(http.StatusOK, `{"access_token":"oauth-issued-token","expires_in":3600}`, nil)
		},
	}
	issued, err := (KISMockOAuthClient{Transport: transport, Timeout: time.Second}).Issue(context.Background(), "app-key-for-test", "app-secret-for-test")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issued.AccessToken != "oauth-issued-token" || issued.ExpiresIn != 3600 {
		t.Fatalf("issued = %#v", issued)
	}
	if transport.calls != 1 || transport.request.Method != http.MethodPost {
		t.Fatalf("request count/method = %d/%q", transport.calls, transport.request.Method)
	}
	if transport.request.URL.String() != kismockread.MockBaseURL+oauthTokenPath {
		t.Fatalf("request URL = %q", transport.request.URL)
	}
	if kismockread.ValidatePinnedURL(transport.request.URL) != nil {
		t.Fatal("OAuth request was not VTS-pinned")
	}
	if transport.request.Header.Get("content-type") != "application/json" ||
		!strings.Contains(transport.body, `"grant_type":"client_credentials"`) ||
		!strings.Contains(transport.body, `"appkey":"app-key-for-test"`) ||
		!strings.Contains(transport.body, `"appsecret":"app-secret-for-test"`) {
		t.Fatal("OAuth request body does not use the VTS client-credentials schema")
	}
}

func TestKISLiveOAuthClientUsesPinnedLivePost(t *testing.T) {
	transport := &recordingOAuthTransport{
		respond: func(*http.Request) *http.Response {
			return oauthResponse(http.StatusOK, `{"access_token":"oauth-issued-token","expires_in":3600}`, nil)
		},
	}
	issued, err := (KISLiveOAuthClient{Transport: transport, Timeout: time.Second}).Issue(context.Background(), "app-key-for-test", "app-secret-for-test")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issued.AccessToken != "oauth-issued-token" || issued.ExpiresIn != 3600 {
		t.Fatalf("issued = %#v", issued)
	}
	if transport.calls != 1 || transport.request.URL.String() != KISLiveBaseURL+oauthTokenPath {
		t.Fatalf("request = %d %v", transport.calls, transport.request.URL)
	}
}

func TestKISOAuthClientRejectsCrossProviderHostBeforeTransport(t *testing.T) {
	for _, test := range []struct {
		name     string
		provider TokenProvider
		baseURL  string
	}{
		{name: "live to mock", provider: ProviderKISLive, baseURL: kismockread.MockBaseURL},
		{name: "mock to live", provider: ProviderKISMock, baseURL: KISLiveBaseURL},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingOAuthTransport{
				respond: func(*http.Request) *http.Response {
					t.Fatal("cross-pinned request reached transport")
					return nil
				},
			}
			_, err := (KISOAuthClient{
				Provider:  test.provider,
				BaseURL:   test.baseURL,
				Transport: transport,
				Timeout:   time.Second,
			}).Issue(context.Background(), "app-key-for-test", "app-secret-for-test")
			if err == nil {
				t.Fatal("cross-pinned KIS OAuth client was accepted")
			}
			if transport.calls != 0 {
				t.Fatalf("cross-pinned request reached transport %d times", transport.calls)
			}
		})
	}
}

func TestKISMockOAuthClientRejectsRedirect(t *testing.T) {
	transport := &recordingOAuthTransport{
		respond: func(*http.Request) *http.Response {
			header := make(http.Header)
			header.Set("location", "https://example.test/not-followed")
			return oauthResponse(http.StatusFound, "", header)
		},
	}
	_, err := (KISMockOAuthClient{Transport: transport, Timeout: time.Second}).Issue(context.Background(), "app-key-for-test", "app-secret-for-test")
	if err == nil {
		t.Fatal("redirect was accepted")
	}
	if transport.calls != 1 {
		t.Fatalf("redirect transport calls = %d, want 1", transport.calls)
	}
}

func TestTossOAuthClientMirrorsFormTokenRequest(t *testing.T) {
	transport := &recordingOAuthTransport{
		respond: func(*http.Request) *http.Response {
			return oauthResponse(http.StatusOK, `{"access_token":"toss-issued-token","expires_in":3600}`, nil)
		},
	}
	issued, err := (TossOAuthClient{BaseURL: TossBaseURL, Transport: transport, Timeout: time.Second}).Issue(context.Background(), "toss-client-id-for-test", "toss-client-secret-for-test")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issued.AccessToken != "toss-issued-token" || issued.ExpiresIn != 3600 {
		t.Fatalf("issued = %#v", issued)
	}
	if transport.calls != 1 || transport.request.Method != http.MethodPost || transport.request.URL.String() != TossBaseURL+tossOAuthTokenPath {
		t.Fatalf("request = %d %v %v", transport.calls, transport.request.Method, transport.request.URL)
	}
	if transport.request.Header.Get("content-type") != "application/x-www-form-urlencoded" {
		t.Fatalf("content type = %q", transport.request.Header.Get("content-type"))
	}
	form, err := url.ParseQuery(transport.body)
	if err != nil || form.Get("grant_type") != "client_credentials" ||
		form.Get("client_id") != "toss-client-id-for-test" || form.Get("client_secret") != "toss-client-secret-for-test" {
		t.Fatalf("form = %q, %v", transport.body, err)
	}
}

func TestTossOAuthClientRejectsUnpinnedHostBeforeTransport(t *testing.T) {
	transport := &recordingOAuthTransport{
		respond: func(*http.Request) *http.Response {
			t.Fatal("untrusted Toss request reached transport")
			return nil
		},
	}
	_, err := (TossOAuthClient{BaseURL: "https://example.test", Transport: transport, Timeout: time.Second}).Issue(context.Background(), "toss-client-id-for-test", "toss-client-secret-for-test")
	if err == nil {
		t.Fatal("untrusted Toss base URL was accepted")
	}
	if transport.calls != 0 {
		t.Fatalf("untrusted Toss request reached transport %d times", transport.calls)
	}
}

func oauthResponse(status int, payload string, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(payload)),
	}
}
