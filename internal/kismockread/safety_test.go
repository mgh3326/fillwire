package kismockread

import (
	"bytes"
	"fmt"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestTokenCacheKeyMatchesSpecifiedDerivation(t *testing.T) {
	key, err := TokenCacheKey("https://OPENAPIVTS.KOREAINVESTMENT.COM:29443/", "app-key-for-test")
	if err != nil {
		t.Fatalf("token key: %v", err)
	}
	const want = "kis_mock:openapivts.koreainvestment.com:29443:343e5aa6119a1729:access_token"
	if key != want {
		t.Fatalf("token key mismatch: got %q", key)
	}
}

func TestConfigReadsOnlyDocumentedEnvironmentNames(t *testing.T) {
	values := map[string]string{
		"KIS_MOCK_APP_KEY":    "key",
		"KIS_MOCK_APP_SECRET": "secret",
		"KIS_MOCK_ACCOUNT_NO": "12345678-01",
		"REDIS_URL":           "redis://cache.example:6379/0",
	}
	var requested []string
	config, err := ConfigFromEnv(func(name string) string {
		requested = append(requested, name)
		return values[name]
	})
	if err != nil || config.BaseURL != MockBaseURL {
		t.Fatalf("config: %v", err)
	}
	sort.Strings(requested)
	want := []string{"KIS_MOCK_ACCOUNT_NO", "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "REDIS_URL"}
	if !reflect.DeepEqual(requested, want) {
		t.Fatalf("environment names = %v", requested)
	}
}

func TestCLIJSONFailureUsesClosedSafeOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := RunCLI(
		context.Background(),
		[]string{"domestic-balance", "--json"},
		func(string) string { return "" },
		&stdout,
		&stderr,
	)
	if exitCode != 1 || stderr.Len() != 0 {
		t.Fatalf("unexpected CLI failure handling: exit=%d stderr=%q", exitCode, stderr.String())
	}
	var output Output
	if json.Unmarshal(stdout.Bytes(), &output) != nil {
		t.Fatal("CLI did not emit JSON")
	}
	if output.Status != "error" || output.ErrorCode != CodeConfigurationMissing || output.Operation != OperationDomesticBalance {
		t.Fatalf("unexpected CLI output: %+v", output)
	}
}

func TestTokenPayloadIsStrictAndBuffered(t *testing.T) {
	now := testNow()
	validToken := redactionProbeToken()
	tests := []struct {
		name string
		raw  string
		want ErrorCode
	}{
		{
			name: "extra field",
			raw:  `{"access_token":"x","expires_at":2000000800,"extra":true}`,
			want: CodeTokenInvalid,
		},
		{
			name: "string expiry",
			raw:  `{"access_token":"x","expires_at":"2000000800"}`,
			want: CodeTokenInvalid,
		},
		{
			name: "duplicate token field",
			raw:  `{"access_token":"x","access_token":"y","expires_at":2000000800}`,
			want: CodeTokenInvalid,
		},
		{
			name: "at expiry buffer boundary",
			raw:  cachedTokenPayload(validToken, float64(now.Add(tokenExpiryBuffer).Unix())),
			want: CodeTokenExpired,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadCachedToken(context.Background(), &staticGetter{value: test.raw, present: true}, "key", now)
			if err == nil || err.Code != test.want {
				t.Fatalf("want %s, got %v", test.want, err)
			}
		})
	}
}

func TestAllowlistContainsOnlyReadTRs(t *testing.T) {
	specs := AllowedReadSpecs()
	if len(specs) != 3 {
		t.Fatalf("allowlist size = %d", len(specs))
	}
	for _, spec := range specs {
		if !strings.HasSuffix(spec.TRID, "R") {
			t.Fatalf("non-read TR in allowlist: %s", spec.TRID)
		}
		if !strings.HasPrefix(spec.Path, "/uapi/") || !strings.Contains(spec.Path, "/trading/") {
			t.Fatalf("unexpected read path: %s", spec.Path)
		}
	}
}

func TestNoTokenIssuerOrRedisMutationFilesExist(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Clean(filepath.Join(workingDirectory, "../.."))
	for _, relativePath := range []string{
		"internal/kismockread/token_issuer.go",
		"internal/kismockread/token_refresh.go",
		"internal/kismockread/redis_write.go",
		"internal/kismockread/redis_lock.go",
	} {
		if _, err := os.Stat(filepath.Join(repositoryRoot, relativePath)); !os.IsNotExist(err) {
			t.Errorf("forbidden file exists or cannot be checked: %s", relativePath)
		}
	}
}

func TestHostPinBlocksBeforeTransport(t *testing.T) {
	for _, rawURL := range []string{
		"http://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-balance",
		"https://other.example:29443/uapi/domestic-stock/v1/trading/inquire-balance",
	} {
		t.Run(rawURL, func(t *testing.T) {
			transport := &recordingTransport{
				respond: func(*http.Request) (*http.Response, error) {
					return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[]}`, nil), nil
				},
			}
			client := NewPinnedHTTPClient(transport, time.Second)
			request, err := http.NewRequest(http.MethodGet, rawURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = client.Do(request)
			if transport.calls != 0 {
				t.Fatalf("blocked URL reached transport %d times", transport.calls)
			}
		})
	}
}

func TestDefaultPinnedTransportDoesNotUseAmbientProxy(t *testing.T) {
	client := NewPinnedHTTPClient(nil, time.Second)
	pinned, ok := client.Transport.(pinningTransport)
	if !ok {
		t.Fatal("missing pinning transport")
	}
	transport, ok := pinned.base.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatal("default pinned client inherited proxy routing")
	}
}

func TestFinalRequestURLIsRevalidatedAtTransport(t *testing.T) {
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[]}`, nil), nil
		},
	}
	client := NewPinnedHTTPClient(transport, time.Second)
	request, err := http.NewRequest(http.MethodGet, MockBaseURL+"/uapi/domestic-stock/v1/trading/inquire-balance", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.URL.Host = "other.example:29443"
	_, _ = client.Do(request)
	if transport.calls != 0 {
		t.Fatal("mutated request reached transport")
	}
}

func TestRedirectIsNeverFollowed(t *testing.T) {
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Location", "https://other.example/elsewhere")
			return jsonResponse(http.StatusFound, "", header), nil
		},
	}
	client := NewPinnedHTTPClient(transport, time.Second)
	request, err := http.NewRequest(http.MethodGet, MockBaseURL+"/uapi/domestic-stock/v1/trading/inquire-balance", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if !errors.Is(err, errRedirectBlocked) {
		t.Fatalf("want redirect block, got %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("redirect caused %d transport calls", transport.calls)
	}
}

func TestExecuteRejectsRedirectResponse(t *testing.T) {
	getter := &staticGetter{value: cachedTokenPayload(redactionProbeToken(), float64(testNow().Add(2*time.Hour).Unix())), present: true}
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Location", "https://other.example/elsewhere")
			return jsonResponse(http.StatusFound, "", header), nil
		},
	}
	_, err := (Executor{TokenGetter: getter, Transport: transport, Now: testNow}).Execute(
		context.Background(), testConfig(), ReadRequest{Operation: OperationDomesticBalance},
	)
	if err == nil || err.Code != CodeRedirectBlocked {
		t.Fatalf("want redirect block, got %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("redirect caused %d transport calls", transport.calls)
	}
}

func TestInvalidPinnedConfigStopsBeforeCacheOrKIS(t *testing.T) {
	getter := &staticGetter{value: cachedTokenPayload(redactionProbeToken(), float64(testNow().Add(2*time.Hour).Unix())), present: true}
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[]}`, nil), nil
		},
	}
	config := testConfig()
	config.BaseURL = "https://other.example:29443"
	_, err := (Executor{TokenGetter: getter, Transport: transport, Now: testNow}).Execute(
		context.Background(), config, ReadRequest{Operation: OperationDomesticBalance},
	)
	if err == nil || err.Code != CodeRequestBlocked {
		t.Fatalf("want request block, got %v", err)
	}
	if getter.calls != 0 || transport.calls != 0 {
		t.Fatalf("invalid config called cache=%d transport=%d", getter.calls, transport.calls)
	}
}

func TestExecuteBuildsPinnedGETFromAllowlist(t *testing.T) {
	token := redactionProbeToken()
	getter := &staticGetter{value: cachedTokenPayload(token, float64(testNow().Add(2*time.Hour).Unix())), present: true}
	transport := &recordingTransport{
		respond: func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				t.Error("request method was not GET")
			}
			if request.URL.Scheme != "https" || request.URL.Host != MockHost {
				t.Error("request was not pinned to VTS")
			}
			if request.URL.Path != "/uapi/domestic-stock/v1/trading/inquire-balance" {
				t.Error("unexpected allowlisted path")
			}
			if request.Header.Get("tr_id") != "VTTC8434R" {
				t.Error("unexpected TR ID")
			}
			if request.Header.Get("authorization") != "Bearer "+token {
				t.Error("authorization was not constructed")
			}
			return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[{},{}]}`, nil), nil
		},
	}
	result, err := (Executor{TokenGetter: getter, Transport: transport, Now: testNow}).Execute(
		context.Background(), testConfig(), ReadRequest{Operation: OperationDomesticBalance},
	)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Operation != OperationDomesticBalance || result.TRID != "VTTC8434R" || result.Pages != 1 || result.Records != 2 {
		t.Fatalf("unexpected summary: %+v", result)
	}
}

func TestOutputSchemaIsClosedAndRedactsTokens(t *testing.T) {
	token := redactionProbeToken()
	getter := &staticGetter{value: cachedTokenPayload(token, float64(testNow().Add(2*time.Hour).Unix())), present: true}
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[{"opaque":"`+token+`"}]}`, nil), nil
		},
	}
	result, err := (Executor{TokenGetter: getter, Transport: transport, Now: testNow}).Execute(
		context.Background(), testConfig(), ReadRequest{Operation: OperationDomesticBalance},
	)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	encoded, marshalErr := json.Marshal(successOutput(result))
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded), token) {
		t.Fatal("token appeared in JSON output")
	}
	var human bytes.Buffer
	writeHumanSuccess(&human, result)
	if strings.Contains(human.String(), token) {
		t.Fatal("token appeared in human output")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil {
		t.Fatal("invalid JSON output")
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	wantKeys := []string{"error_code", "operation", "pages", "records", "schema_version", "status", "tr_id"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("JSON schema keys = %v", keys)
	}
}

func TestFailureOutputNeverEchoesTokenOrBrokerMessage(t *testing.T) {
	token := redactionProbeToken()
	getter := &staticGetter{value: cachedTokenPayload(token, float64(testNow().Add(2*time.Hour).Unix())), present: true}
	transport := &recordingTransport{
		respond: func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"rt_cd":"1","msg1":"`+token+`","output1":[]}`, nil), nil
		},
	}
	_, err := (Executor{TokenGetter: getter, Transport: transport, Now: testNow}).Execute(
		context.Background(), testConfig(), ReadRequest{Operation: OperationDomesticBalance},
	)
	if err == nil || err.Code != CodeBrokerRejected {
		t.Fatalf("want broker rejection, got %v", err)
	}
	encoded, marshalErr := json.Marshal(failureOutput(OperationDomesticBalance, err.Code))
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(encoded)+err.Error(), token) {
		t.Fatal("token appeared in failure output")
	}
	var human bytes.Buffer
	writeHumanFailure(&human, err.Code)
	if strings.Contains(human.String(), token) {
		t.Fatal("token appeared in human error")
	}
}

func TestValidatePinnedURLRejectsHTTPAndAlternateHost(t *testing.T) {
	for _, rawURL := range []string{
		"http://openapivts.koreainvestment.com:29443",
		"https://other.example:29443",
	} {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			t.Fatal(err)
		}
		if ValidatePinnedURL(parsed) == nil {
			t.Fatalf("accepted %s", rawURL)
		}
	}
}

func TestCachedTokenAcceptsPythonWriterShapeAndRejectsUnknownKeys(t *testing.T) {
	// The live Python writer stores three fields; the first live witness
	// failed with token_invalid because the parser demanded exactly two.
	future := float64(time.Now().Unix() + 3600)
	pythonShaped := fmt.Sprintf(
		`{"access_token":"tok-abc","expires_at":%f,"created_at":%f}`,
		future, future-3600,
	)
	getter := &staticGetter{value: pythonShaped, present: true}
	token, err := LoadCachedToken(context.Background(), getter, "k", time.Now())
	if err != nil || token != "tok-abc" {
		t.Fatalf("python-shaped cache must parse: token=%q err=%v", token, err)
	}
	unknown := fmt.Sprintf(
		`{"access_token":"tok-abc","expires_at":%f,"refresh_token":"x"}`, future,
	)
	getter2 := &staticGetter{value: unknown, present: true}
	if _, err := LoadCachedToken(context.Background(), getter2, "k", time.Now()); err == nil {
		t.Fatal("unknown key must stay rejected")
	}
}
