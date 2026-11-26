package gatewayd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testFetchCredential      = "fetch-credential-for-tests-0123456789"
	testFetchOtherCredential = "other-credential-for-tests-9876543210"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.t
}

func (clock *testClock) advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.t = clock.t.Add(duration)
}

// countingStore counts every Redis operation so a test can show that a memory
// hit makes none, and that the value+TTL read is one operation.
type countingStore struct {
	*fakeRedisStore
	countMu sync.Mutex
	counts  map[string]int
}

func newCountingStore(now func() time.Time) *countingStore {
	return &countingStore{fakeRedisStore: newFakeRedisStore(now), counts: make(map[string]int)}
}

func (store *countingStore) count(op string) {
	store.countMu.Lock()
	store.counts[op]++
	store.countMu.Unlock()
}

func (store *countingStore) total() int {
	store.countMu.Lock()
	defer store.countMu.Unlock()
	sum := 0
	for _, n := range store.counts {
		sum += n
	}
	return sum
}

func (store *countingStore) of(op string) int {
	store.countMu.Lock()
	defer store.countMu.Unlock()
	return store.counts[op]
}

func (store *countingStore) Get(ctx context.Context, key string) (string, bool, error) {
	store.count("get")
	return store.fakeRedisStore.Get(ctx, key)
}

func (store *countingStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	store.count("set")
	return store.fakeRedisStore.Set(ctx, key, value, ttl)
}

func (store *countingStore) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	store.count("setnx")
	return store.fakeRedisStore.SetNX(ctx, key, value, ttl)
}

func (store *countingStore) CompareAndDelete(ctx context.Context, key, value string) error {
	store.count("cad")
	return store.fakeRedisStore.CompareAndDelete(ctx, key, value)
}

func (store *countingStore) TTL(ctx context.Context, key string) (time.Duration, error) {
	store.count("ttl")
	return store.fakeRedisStore.TTL(ctx, key)
}

func (store *countingStore) Held(ctx context.Context, key string) (bool, error) {
	store.count("held")
	return store.fakeRedisStore.Held(ctx, key)
}

// ReadWithTTL mirrors the Lua script: value and remaining TTL under one lock.
func (store *countingStore) ReadWithTTL(_ context.Context, key string) (string, time.Duration, bool, error) {
	store.count("read")
	store.fakeRedisStore.mu.Lock()
	defer store.fakeRedisStore.mu.Unlock()
	if store.fakeRedisStore.getErr != nil {
		return "", 0, false, store.fakeRedisStore.getErr
	}
	value, found := store.fakeRedisStore.values[key]
	now := store.fakeRedisStore.now()
	if !found || (!value.expiresAt.IsZero() && !now.Before(value.expiresAt)) {
		return "", 0, false, nil
	}
	if value.expiresAt.IsZero() {
		return value.value, -1, true, nil
	}
	return value.value, value.expiresAt.Sub(now), true, nil
}

// lockedLogs is a concurrency-safe EventLogger (log.Logger is safe in
// production; the shared bytes.Buffer used by other tests is not).
type lockedLogs struct {
	mu    sync.Mutex
	lines []string
}

func (logs *lockedLogs) Print(values ...any) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	for _, value := range values {
		if text, ok := value.(string); ok {
			logs.lines = append(logs.lines, text)
		}
	}
}

func (logs *lockedLogs) String() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return strings.Join(logs.lines, "\n")
}

type fetchHarness struct {
	t        *testing.T
	provider TokenProvider
	clock    *testClock
	store    *countingStore
	issuer   *fakeOAuthIssuer
	ensurer  fetchSource
	fetch    *FetchService
	handler  http.Handler
	logs     *lockedLogs
	metrics  *Metrics
}

func newFetchHarness(t *testing.T, provider TokenProvider, consumers ...FetchConsumer) *fetchHarness {
	t.Helper()
	clock := &testClock{t: testGatewayNow()}
	store := newCountingStore(clock.Now)
	issuer := &fakeOAuthIssuer{}
	var ensurer fetchSource
	if isApprovalProvider(provider) {
		prefix := "kis:websocket:approval_key"
		if provider == ProviderKISMockApproval {
			prefix = "kis_mock:websocket:approval_key"
		}
		ensurer = &approvalService{store: store, config: testProviderConfig(provider), issuer: issuer, cacheKey: prefix, lockKey: prefix + ":lock"}
	} else {
		service, err := NewEnsureServiceForProvider(store, issuer, testProviderConfig(provider))
		if err != nil {
			t.Fatal(err)
		}
		service.Now = clock.Now
		service.InitialLockWait = time.Millisecond
		service.LockPollEvery = time.Millisecond
		service.LockWaitTimeout = 5 * time.Millisecond
		ensurer = service
	}
	if len(consumers) == 0 {
		consumers = []FetchConsumer{{Name: "at", Credential: testFetchCredential, Providers: orderedProviders()}}
	}
	ensurers := ProviderEnsurers{provider: ensurer}
	fetch := NewFetchService(ensurers, store, consumers)
	fetch.now = clock.Now
	logs := &lockedLogs{}
	metrics := NewMetrics()
	return &fetchHarness{
		t: t, provider: provider, clock: clock, store: store, issuer: issuer, ensurer: ensurer, fetch: fetch,
		handler: newHandlerWithFetch(ensurers, logs, metrics, fetch), logs: logs, metrics: metrics,
	}
}

func (h *fetchHarness) reuseSource() string {
	if h.provider == ProviderToss {
		return "redis"
	}
	return "memory"
}

func (h *fetchHarness) issue(token string) {
	h.issuer.mu.Lock()
	defer h.issuer.mu.Unlock()
	h.issuer.token = OAuthToken{AccessToken: token, ExpiresIn: 3600}
}

// seed writes a token into Redis the way the provider's Python writer does.
func (h *fetchHarness) seed(token string) {
	h.t.Helper()
	var value string
	var ttl time.Duration
	var err error
	switch {
	case isApprovalProvider(h.provider):
		value, ttl = token, approvalTTL
	case h.provider == ProviderToss:
		value, err = tossTokenPayload(token, 3600, h.clock.Now())
		ttl = 3600 * time.Second
	default:
		value, err = tokenPayload(token, 3600, h.clock.Now())
		ttl = 3660 * time.Second
	}
	if err != nil {
		h.t.Fatal(err)
	}
	h.store.save(h.ensurer.tokenKey(), value, ttl)
}

func (h *fetchHarness) nearExpiry() {
	if isApprovalProvider(h.provider) {
		h.clock.advance(approvalTTL - 30*time.Second)
		return
	}
	h.clock.advance(3600*time.Second - 30*time.Second)
}

type fetchReply struct {
	code   int
	header http.Header
	raw    string
	body   map[string]any
}

func (h *fetchHarness) post(provider TokenProvider, credential, body string) fetchReply {
	h.t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/tokens/"+string(provider)+"/fetch", strings.NewReader(body))
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	response := httptest.NewRecorder()
	h.handler.ServeHTTP(response, request)
	reply := fetchReply{code: response.Code, header: response.Header(), raw: response.Body.String()}
	_ = json.Unmarshal([]byte(reply.raw), &reply.body)
	return reply
}

func (h *fetchHarness) fetchBody(body string) fetchReply {
	return h.post(h.provider, testFetchCredential, body)
}

func shaHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func wantToken(t *testing.T, reply fetchReply, token, source string) {
	t.Helper()
	if reply.code != http.StatusOK {
		t.Fatalf("status = %d body %q, want 200 (token %s)", reply.code, reply.raw, source)
	}
	if reply.body["token"] != token || reply.body["token_sha256"] != shaHex(token) || reply.body["source"] != source {
		t.Fatalf("reply = %v, want token sha %s source %s", reply.body["token_sha256"], shaHex(token), source)
	}
}

var fetchProviders = []TokenProvider{ProviderKISMock, ProviderKISLive, ProviderToss, ProviderKISMockApproval, ProviderKISLiveApproval}

func eachFetchProvider(t *testing.T, run func(t *testing.T, h *fetchHarness)) {
	for _, provider := range fetchProviders {
		t.Run(string(provider), func(t *testing.T) { run(t, newFetchHarness(t, provider)) })
	}
}

func TestFetchContractShapeAndIssueMiss(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.issue("issued-token-one")
		reply := h.fetchBody(`{}`)
		wantToken(t, reply, "issued-token-one", "issued")
		keys := make([]string, 0, len(reply.body))
		for key := range reply.body {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if strings.Join(keys, ",") != "expires_at,provider,source,token,token_sha256" {
			t.Fatalf("keys = %v", keys)
		}
		if reply.body["provider"] != string(h.provider) {
			t.Fatalf("provider = %v", reply.body["provider"])
		}
		if reply.header.Get("Cache-Control") != "no-store" || reply.header.Get("Content-Type") != "application/json" {
			t.Fatalf("headers = %v", reply.header)
		}
		expiresAt, err := time.Parse(time.RFC3339, reply.body["expires_at"].(string))
		if err != nil || !strings.HasSuffix(reply.body["expires_at"].(string), "Z") {
			t.Fatalf("expires_at = %v (%v)", reply.body["expires_at"], err)
		}
		want := h.clock.Now().Add(3600 * time.Second)
		if isApprovalProvider(h.provider) {
			want = h.clock.Now().Add(approvalTTL)
		}
		if expiresAt.Before(want.Add(-2*time.Second)) || expiresAt.After(want) {
			t.Fatalf("expires_at = %v, want about %v", expiresAt, want)
		}
		if h.issuer.callCount() != 1 {
			t.Fatalf("issuer calls = %d, want exactly one existing-path issue", h.issuer.callCount())
		}
	})
}

func TestFetchReusesFreshToken(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.issue("memory-token")
		wantToken(t, h.fetchBody(`{}`), "memory-token", "issued")
		before := h.store.total()
		reads := h.store.of("read")
		for range 3 {
			wantToken(t, h.fetchBody(`{}`), "memory-token", h.reuseSource())
		}
		if h.provider == ProviderToss {
			if got := h.store.of("read") - reads; got != 3 {
				t.Fatalf("Toss fetches read %d shared snapshots, want 3", got)
			}
		} else if h.store.total() != before {
			t.Fatalf("memory hits made %d Redis calls", h.store.total()-before)
		}
		if h.issuer.callCount() != 1 {
			t.Fatalf("issuer calls = %d", h.issuer.callCount())
		}
	})
}

func TestFetchMemoryMissCallsEnsureOnceAndReadsRedisAtomically(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.seed("redis-token")
		reply := h.fetchBody(`{}`)
		wantToken(t, reply, "redis-token", "redis")
		if h.issuer.callCount() != 0 {
			t.Fatalf("fresh Redis token was reissued")
		}
		if h.store.of("read") != 1 {
			t.Fatalf("atomic reads = %d, want 1", h.store.of("read"))
		}
		if h.store.of("set") != 0 {
			t.Fatalf("a fetch of a fresh token wrote Redis")
		}

		// Near expiry the memory entry is unusable: exactly one ensure re-issues.
		h.nearExpiry()
		h.issue("replacement-token")
		wantToken(t, h.fetchBody(`{}`), "replacement-token", "issued")
		if h.issuer.callCount() != 1 {
			t.Fatalf("issuer calls = %d, want 1", h.issuer.callCount())
		}
		wantToken(t, h.fetchBody(`{}`), "replacement-token", h.reuseSource())
	})
}

func TestFetchExpiryComesFromTheAtomicReadTTL(t *testing.T) {
	// A payload that outlives its Redis key must not extend the token's life,
	// and a Redis key that outlives the payload must not either.
	h := newFetchHarness(t, ProviderKISMock)
	payload, err := tokenPayload("ttl-token", 3600, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.store.save(h.ensurer.tokenKey(), payload, 100*time.Second)
	reply := h.fetchBody(`{}`)
	wantToken(t, reply, "ttl-token", "redis")
	expiresAt, _ := time.Parse(time.RFC3339, reply.body["expires_at"].(string))
	if want := h.clock.Now().Add(100 * time.Second).Truncate(time.Second); !expiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want Redis TTL bound %v", expiresAt, want)
	}

	h = newFetchHarness(t, ProviderKISMock)
	h.store.save(h.ensurer.tokenKey(), payload, 7200*time.Second)
	reply = h.fetchBody(`{}`)
	expiresAt, _ = time.Parse(time.RFC3339, reply.body["expires_at"].(string))
	if want := h.clock.Now().Add(3600 * time.Second).Truncate(time.Second); !expiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want payload bound %v", expiresAt, want)
	}
}

func TestFetchFailedHashEqualForcesReissue(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.issue("token-one")
		wantToken(t, h.fetchBody(`{}`), "token-one", "issued")
		h.issue("token-two")
		reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("token-one") + `"}`)
		wantToken(t, reply, "token-two", "issued")
		if h.issuer.callCount() != 2 {
			t.Fatalf("issuer calls = %d, want 2 (a plain ensure would have reused the fresh token)", h.issuer.callCount())
		}
		wantToken(t, h.fetchBody(`{}`), "token-two", h.reuseSource())
	})
}

func TestFetchFailedHashEqualForcesWithColdMemory(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.seed("redis-held")
		h.issue("reissued")
		wantToken(t, h.fetchBody(`{"failed_token_sha256":"`+shaHex("redis-held")+`"}`), "reissued", "issued")
	})
}

func TestFetchFailedHashDifferingReturnsCurrentWithoutReissue(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		stale := `{"failed_token_sha256":"` + shaHex("stale-token") + `"}`
		// Warm memory.
		h.seed("current-token")
		wantToken(t, h.fetchBody(`{}`), "current-token", "redis")
		wantToken(t, h.fetchBody(stale), "current-token", "redis")

		// Cold memory: the current token comes from Redis.
		cold := newFetchHarness(t, h.provider)
		cold.seed("current-token")
		wantToken(t, cold.fetchBody(stale), "current-token", "redis")
		if h.issuer.callCount() != 0 || cold.issuer.callCount() != 0 {
			t.Fatal("a stale caller caused a reissue")
		}
	})
}

func TestFetchRotatedRedisTokenIsServedToACallerHoldingTheMemoryToken(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("first")
	wantToken(t, h.fetchBody(`{}`), "first", "issued")
	// Another issuer rotates the credential; the consumer's report is the signal.
	h.seed("rotated")
	reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("first") + `"}`)
	wantToken(t, reply, "rotated", "redis")
	if h.issuer.callCount() != 1 {
		t.Fatalf("issuer calls = %d, want only the first issue", h.issuer.callCount())
	}
}

func TestFetchFailedTokenStillCurrentAfterEnsureIs503WithoutRetry(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.issue("same-token")
		wantToken(t, h.fetchBody(`{}`), "same-token", "issued")
		// The broker hands the same token back.
		reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("same-token") + `"}`)
		if reply.code != http.StatusServiceUnavailable || reply.raw != `{"error":"issue_unavailable"}` {
			t.Fatalf("unexpected response status or error body (status %d)", reply.code)
		}
		if h.issuer.callCount() != 2 {
			t.Fatalf("issuer calls = %d, want initial issue plus exactly one forced issue", h.issuer.callCount())
		}
		if strings.Contains(reply.raw, "same-token") {
			t.Fatal("error body carried the token")
		}
	})
}

func TestFetchForcedContendedLockIs503(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.seed("held-token")
	wantToken(t, h.fetchBody(`{}`), "held-token", "redis")
	lockKey := h.ensurer.(*EnsureService).LockKey
	h.store.locks[lockKey] = storedValue{value: "other-issuer", expiresAt: h.clock.Now().Add(time.Hour)}
	reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("held-token") + `"}`)
	if reply.code != http.StatusServiceUnavailable || reply.raw != `{"error":"issue_unavailable"}` {
		t.Fatalf("unexpected response status or error body (status %d)", reply.code)
	}
	if h.issuer.callCount() != 0 {
		t.Fatal("issued without owning the lock")
	}
}

func TestFetchIssueFailureIs503(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issuer.err = context.DeadlineExceeded
	reply := h.fetchBody(`{}`)
	if reply.code != http.StatusServiceUnavailable || reply.raw != `{"error":"issue_unavailable"}` {
		t.Fatalf("unexpected response status or error body (status %d)", reply.code)
	}
}

func TestFetchConcurrentFailedReportsReissueOnce(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("shared-old")
	wantToken(t, h.fetchBody(`{}`), "shared-old", "issued")
	h.issue("shared-new")
	body := `{"failed_token_sha256":"` + shaHex("shared-old") + `"}`
	var wait sync.WaitGroup
	replies := make([]fetchReply, 8)
	for index := range replies {
		wait.Add(1)
		go func() {
			defer wait.Done()
			replies[index] = h.fetchBody(body)
		}()
	}
	wait.Wait()
	for _, reply := range replies {
		wantToken(t, reply, "shared-new", reply.body["source"].(string))
	}
	if h.issuer.callCount() != 2 {
		t.Fatalf("issuer calls = %d, want 2 (one initial, one shared reissue)", h.issuer.callCount())
	}
}

func TestFetchAuthAndAllowlist(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock, FetchConsumer{Name: "at", Credential: testFetchCredential, Providers: []TokenProvider{ProviderKISMock}},
		FetchConsumer{Name: "other", Credential: testFetchOtherCredential, Providers: []TokenProvider{ProviderToss}})
	h.issue("auth-token")
	for _, test := range []struct {
		name       string
		provider   TokenProvider
		credential string
		code       int
		body       string
	}{
		{"missing credential", ProviderKISMock, "", 401, `{"error":"unauthenticated"}`},
		{"wrong credential", ProviderKISMock, "wrong-credential-wrong-credential-1234", 401, `{"error":"unauthenticated"}`},
		{"unknown provider authenticated", "bogus", testFetchCredential, 404, `{"error":"unknown_provider"}`},
		{"unknown provider unauthenticated", "bogus", "", 401, `{"error":"unauthenticated"}`},
		{"provider outside allowlist", ProviderToss, testFetchCredential, 403, `{"error":"provider_not_allowed"}`},
		{"allowed but not configured here", ProviderToss, testFetchOtherCredential, 404, `{"error":"unknown_provider"}`},
		{"other consumer denied kis-mock", ProviderKISMock, testFetchOtherCredential, 403, `{"error":"provider_not_allowed"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := h.store.total()
			reply := h.post(test.provider, test.credential, `{}`)
			if reply.code != test.code || reply.raw != test.body {
				t.Fatalf("reply = %d %q, want %d %q", reply.code, reply.raw, test.code, test.body)
			}
			if h.store.total() != before || h.issuer.callCount() != 0 {
				t.Fatal("a rejected request reached Redis or the broker")
			}
		})
	}
	wantToken(t, h.post(ProviderKISMock, testFetchCredential, `{}`), "auth-token", "issued")

	// Malformed Authorization schemes never authenticate.
	for _, header := range []string{testFetchCredential, "Basic " + testFetchCredential, "Bearer", "Bearer  " + testFetchCredential, "Bearer " + testFetchCredential + " x"} {
		request := httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/fetch", strings.NewReader(`{}`))
		request.Header.Set("Authorization", header)
		response := httptest.NewRecorder()
		h.handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("Authorization %q = %d", header, response.Code)
		}
	}
}

func TestFetchIsOffByDefault(t *testing.T) {
	clock := &testClock{t: testGatewayNow()}
	store := newCountingStore(clock.Now)
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "never-served", ExpiresIn: 3600}}
	service := newTestEnsureService(t, store, issuer, clock.Now())
	for name, handler := range map[string]http.Handler{
		"nil fetch service":      newHandlerWithFetch(ProviderEnsurers{ProviderKISMock: service}, nil, nil, nil),
		"no consumers":           newHandlerWithFetch(ProviderEnsurers{ProviderKISMock: service}, nil, nil, NewFetchService(ProviderEnsurers{ProviderKISMock: service}, store, nil)),
		"NewHandler convenience": NewHandler(ProviderEnsurers{ProviderKISMock: service}, nil),
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/fetch", strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer "+testFetchCredential)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || response.Body.String() != `{"error":"unauthenticated"}` {
			t.Fatalf("%s: %d %q", name, response.Code, response.Body.String())
		}
	}
	if store.total() != 0 || issuer.callCount() != 0 {
		t.Fatal("an unauthenticated fetch reached Redis or the broker")
	}
}

func TestFetchRejectsBadBodies(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("body-token")
	good := shaHex("x")
	for name, body := range map[string]string{
		"empty":             ``,
		"null":              `null`,
		"array":             `[]`,
		"unknown field":     `{"failed_token_sha256":"` + good + `","extra":1}`,
		"only unknown":      `{"force":true}`,
		"uppercase hash":    `{"failed_token_sha256":"` + strings.ToUpper(good) + `"}`,
		"short hash":        `{"failed_token_sha256":"abc"}`,
		"null hash":         `{"failed_token_sha256":null}`,
		"duplicate hash":    `{"failed_token_sha256":"` + good + `","failed_token_sha256":"` + good + `"}`,
		"non-string hash":   `{"failed_token_sha256":5}`,
		"trailing json":     `{} {}`,
		"trailing garbage":  `{}x`,
		"oversized":         `{"failed_token_sha256":"` + strings.Repeat("a", 4096) + `"}`,
		"non-hex 64 chars":  `{"failed_token_sha256":"` + strings.Repeat("g", 64) + `"}`,
		"plain text":        `failed_token_sha256`,
		"not an object str": `"` + good + `"`,
	} {
		t.Run(name, func(t *testing.T) {
			reply := h.fetchBody(body)
			if reply.code != http.StatusBadRequest || reply.raw != `{"error":"bad_request"}` {
				t.Fatalf("unexpected response status or error body (status %d)", reply.code)
			}
		})
	}
	if h.store.total() != 0 || h.issuer.callCount() != 0 {
		t.Fatal("a bad request reached Redis or the broker")
	}
	wantToken(t, h.fetchBody(`{"failed_token_sha256":"`+good+`"}`), "body-token", "issued")
}

type panickingFetchSource struct{ message string }

func (source panickingFetchSource) Ensure(context.Context) (EnsureState, error) {
	panic(source.message)
}

func (source panickingFetchSource) EnsureReplacing(context.Context, string) (EnsureState, error) {
	panic(source.message)
}
func (source panickingFetchSource) tokenKey() string { return "panic-key" }

func TestFetchNeverEmitsTokenInLogsMetricsErrorsOrPanics(t *testing.T) {
	const probe = "SECRET-token-probe-4f9a1c7e"
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		var seen []string
		record := func(reply fetchReply) { seen = append(seen, reply.raw) }
		h.issue(probe)
		success := h.fetchBody(`{}`)
		wantToken(t, success, probe, "issued")
		wantToken(t, h.fetchBody(`{}`), probe, h.reuseSource())
		record(h.fetchBody(`{"failed_token_sha256":"` + shaHex(probe) + `"}`)) // 503: same token again
		record(h.fetchBody(`{"extra":"` + probe + `"}`))                       // 400
		record(h.post(h.provider, "not-the-credential-"+probe, `{}`))          // 401
		h.issuer.err = context.DeadlineExceeded
		h.issuer.token = OAuthToken{}
		h.nearExpiry()
		record(h.fetchBody(`{}`)) // 503: issue failed

		metrics := httptest.NewRecorder()
		h.handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		combined := strings.Join(seen, "\n") + "\n" + h.logs.String() + "\n" + metrics.Body.String()
		if strings.Contains(combined, probe) || strings.Contains(combined, shaHex(probe)) {
			t.Fatalf("token material leaked into an error body, log or metric:\n%s", h.logs.String())
		}
		if !strings.Contains(metrics.Body.String(), `gatewayd_fetch_results_total{provider="`+string(h.provider)+`",result="issued"} 1`) {
			t.Fatalf("fetch metric missing:\n%s", metrics.Body.String())
		}
	})

	var logs bytes.Buffer
	panicking := ProviderEnsurers{ProviderKISMock: panickingFetchSource{message: "boom " + probe}}
	handler := newHandlerWithFetch(panicking, testLogger(&logs), nil,
		NewFetchService(panicking, newCountingStore(time.Now), []FetchConsumer{{Name: "at", Credential: testFetchCredential, Providers: []TokenProvider{ProviderKISMock}}}))
	request := httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/fetch", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+testFetchCredential)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != `{"error":"issue_unavailable"}` {
		t.Fatalf("panic reply = %d %q", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String()+logs.String(), probe) {
		t.Fatalf("panic text leaked: %q", logs.String())
	}
}

func TestFetchLogLinesAreFixedText(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("log-token")
	h.fetchBody(`{}`)
	h.fetchBody(`{}`)
	h.post(ProviderKISMock, "", `{}`)
	allowed := regexp.MustCompile(`^gatewayd: token fetch (issued|memory|redis|failed|unauthenticated|bad request|unknown provider|provider not allowed)$`)
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		if !allowed.MatchString(line) {
			t.Fatalf("unexpected log line %q", line)
		}
	}
}

func TestEnsureRouteIsUnchangedByFetch(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("ensure-token")
	response := httptest.NewRecorder()
	h.handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/ensure", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"state":"issued"}` {
		t.Fatalf("ensure = %d %q", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	h.handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/ensure", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"state":"fresh"}` || response.Header().Get("Cache-Control") != "" {
		t.Fatalf("ensure = %d %q %v", response.Code, response.Body.String(), response.Header())
	}
}

func TestEnsureReplacingSkipsFreshnessButKeepsTheLock(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	service := h.ensurer.(*EnsureService)
	h.seed("fresh-token")
	h.issue("forced-token")
	if state, err := service.Ensure(context.Background()); err != nil || state != EnsureStateFresh || h.issuer.callCount() != 0 {
		t.Fatalf("Ensure = %v %v calls %d", state, err, h.issuer.callCount())
	}
	if state, err := service.EnsureReplacing(context.Background(), ""); err != nil || state != EnsureStateIssued || h.issuer.callCount() != 1 {
		t.Fatalf("EnsureForce = %v %v calls %d", state, err, h.issuer.callCount())
	}
	if locked := h.store.savedWhileLocked(); len(locked) != 1 || !locked[0] {
		t.Fatalf("forced publish was not made under the lock: %v", locked)
	}
}

func TestParseFetchConsumers(t *testing.T) {
	valid := `[{"name":"at","credential":"` + testFetchCredential + `","providers":["kis-mock","toss"]}]`
	consumers, err := parseFetchConsumers(valid)
	if err != nil || len(consumers) != 1 || consumers[0].Name != "at" || len(consumers[0].Providers) != 2 {
		t.Fatal("valid consumer config rejected")
	}
	if consumers, err := parseFetchConsumers("  "); err != nil || consumers != nil {
		t.Fatal("blank consumer config rejected")
	}
	if consumers, err := parseFetchConsumers("[]"); err != nil || len(consumers) != 0 {
		t.Fatal("empty array rejected")
	}
	for name, raw := range map[string]string{
		"null":               `null`,
		"trailing bracket":   valid + `]`,
		"combined provider":  strings.Replace(valid, `"kis-mock","toss"`, `"kis-mock,toss"`, 1),
		"not json":           `nope`,
		"object":             `{"name":"at"}`,
		"unknown field":      `[{"name":"at","credential":"` + testFetchCredential + `","providers":["kis-mock"],"x":1}]`,
		"short credential":   `[{"name":"at","credential":"short","providers":["kis-mock"]}]`,
		"space in secret":    `[{"name":"at","credential":"` + strings.Repeat("a", 20) + ` ` + strings.Repeat("b", 20) + `","providers":["kis-mock"]}]`,
		"no providers":       `[{"name":"at","credential":"` + testFetchCredential + `","providers":[]}]`,
		"unknown provider":   `[{"name":"at","credential":"` + testFetchCredential + `","providers":["nope"]}]`,
		"no name":            `[{"credential":"` + testFetchCredential + `","providers":["kis-mock"]}]`,
		"duplicate name":     `[{"name":"a","credential":"` + testFetchCredential + `","providers":["kis-mock"]},{"name":"a","credential":"` + testFetchOtherCredential + `","providers":["kis-mock"]}]`,
		"duplicate secret":   `[{"name":"a","credential":"` + testFetchCredential + `","providers":["kis-mock"]},{"name":"b","credential":"` + testFetchCredential + `","providers":["kis-mock"]}]`,
		"trailing JSON":      valid + ` []`,
		"duplicate provider": `[{"name":"a","credential":"` + testFetchCredential + `","providers":["toss","toss"]}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFetchConsumers(raw); err == nil {
				t.Fatal("accepted")
			} else if strings.Contains(err.Error(), testFetchCredential) {
				t.Fatal("error carried the credential")
			}
		})
	}

	lookup := func(name string) string {
		switch name {
		case "REDIS_URL":
			return "redis://127.0.0.1:6379/0"
		case "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET":
			return "value"
		case "GATEWAYD_FETCH_CONSUMERS":
			return valid
		}
		return ""
	}
	config, err := ConfigFromEnv(lookup)
	if err != nil || len(config.FetchConsumers) != 1 {
		t.Fatal("ConfigFromEnv rejected valid consumers")
	}
	if _, err := ConfigFromEnv(func(name string) string {
		if name == "GATEWAYD_FETCH_CONSUMERS" {
			return "garbage"
		}
		return lookup(name)
	}); err == nil {
		t.Fatal("invalid consumers accepted by ConfigFromEnv")
	}
}

func TestRedisClientReadWithTTLIsOneScriptedRead(t *testing.T) {
	for _, test := range []struct {
		name    string
		reply   string
		value   string
		ttl     time.Duration
		present bool
		wantErr bool
	}{
		{name: "value and ttl", reply: "*2\r\n$5\r\nvalue\r\n:61000\r\n", value: "value", ttl: 61 * time.Second, present: true},
		{name: "missing key", reply: "$-1\r\n"},
		{name: "resp3 null", reply: "_\r\n"},
		{name: "no expiry", reply: "*2\r\n$5\r\nvalue\r\n:-1\r\n", value: "value", ttl: -time.Millisecond, present: true},
		{name: "missing sentinel", reply: "*2\r\n$5\r\nvalue\r\n:-2\r\n"},
		{name: "invalid negative", reply: "*2\r\n$5\r\nvalue\r\n:-3\r\n", wantErr: true},
		{name: "overflowing ttl", reply: "*2\r\n$5\r\nvalue\r\n:9223372036855\r\n", wantErr: true},
		{name: "wrong length", reply: "*1\r\n$5\r\nvalue\r\n", wantErr: true},
		{name: "ttl not an integer", reply: "*2\r\n$5\r\nvalue\r\n$2\r\n61\r\n", wantErr: true},
		{name: "nested array", reply: "*2\r\n*1\r\n$1\r\nx\r\n:1\r\n", wantErr: true},
		{name: "error reply", reply: "-ERR nope\r\n", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			commands := make(chan []string, 1)
			go func() {
				connection, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				defer connection.Close()
				arguments, readErr := readTestRESPArray(bufio.NewReader(connection))
				if readErr == nil {
					commands <- arguments
					_, _ = connection.Write([]byte(test.reply))
				}
			}()
			client, err := NewRedisClient("redis://" + listener.Addr().String() + "/0")
			if err != nil {
				t.Fatal(err)
			}
			value, ttl, present, err := client.ReadWithTTL(context.Background(), "token-key")
			if (err != nil) != test.wantErr || value != test.value || ttl != test.ttl || present != test.present {
				t.Fatalf("ReadWithTTL = %q %v %t %v", value, ttl, present, err)
			}
			command := <-commands
			if len(command) != 4 || command[0] != "EVAL" || command[2] != "1" || command[3] != "token-key" ||
				!strings.Contains(command[1], "GET") || !strings.Contains(command[1], "PTTL") {
				t.Fatalf("command = %q, want one EVAL doing GET and PTTL", command)
			}
		})
	}
}

// A peer can complete a full issue cycle between the snapshot that decided to
// force and the lock acquisition. The forced path must then adopt the peer's
// token instead of issuing a second one (which, for Toss, kills the first).
func TestFetchForcedReissueYieldsToAPeerThatPublishedBeforeTheLock(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.seed("rejected-token")
		wantToken(t, h.fetchBody(`{}`), "rejected-token", "redis")
		h.issue("must-not-be-issued")
		var once sync.Once
		h.store.setNXHook = func() { once.Do(func() { h.seed("peer-token") }) }
		reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("rejected-token") + `"}`)
		wantToken(t, reply, "peer-token", "redis")
		if h.issuer.callCount() != 0 {
			t.Fatalf("issuer calls = %d, want 0: the forced path replaced a token a peer had just published", h.issuer.callCount())
		}
		wantToken(t, h.fetchBody(`{}`), "peer-token", h.reuseSource())
	})
}

// A failure report is checked against Redis even when memory holds a token
// that differs from the reported one (memory can be stale: /ensure?force=true
// or a peer may have replaced the key).
func TestFetchFailureReportChecksRedisInsteadOfTrustingMemory(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.seed("token-b")
		wantToken(t, h.fetchBody(`{}`), "token-b", "redis")
		h.seed("token-c") // published behind the cache's back
		reads := h.store.of("read")
		reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("token-a") + `"}`)
		wantToken(t, reply, "token-c", "redis")
		if h.store.of("read") == reads {
			t.Fatal("a failure report was answered without reading Redis")
		}
		if h.issuer.callCount() != 0 {
			t.Fatal("a stale report caused a reissue")
		}
	})
}

// Overlapping reports of the same failed token share one attempt, including
// its failure; a later independent request may retry.
func TestFetchOverlappingReportsShareTheAttemptAndItsFailure(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("token-a")
	wantToken(t, h.fetchBody(`{}`), "token-a", "issued")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h.issuer.mu.Lock()
	h.issuer.started, h.issuer.release = started, release
	h.issuer.mu.Unlock()
	body := `{"failed_token_sha256":"` + shaHex("token-a") + `"}`
	replies := make(chan fetchReply, 2)
	go func() { replies <- h.fetchBody(body) }()
	<-started // the leader is inside the broker call
	go func() { replies <- h.fetchBody(body) }()
	time.Sleep(100 * time.Millisecond) // let the follower join the attempt
	close(release)
	for range 2 {
		if reply := <-replies; reply.code != http.StatusServiceUnavailable || reply.raw != `{"error":"issue_unavailable"}` {
			t.Fatalf("unexpected response status or error body (status %d)", reply.code)
		}
	}
	if h.issuer.callCount() != 2 {
		t.Fatalf("issuer calls = %d, want initial plus one shared forced issue", h.issuer.callCount())
	}
	// A later independent request is eligible to retry.
	if reply := h.fetchBody(body); reply.code != http.StatusServiceUnavailable || h.issuer.callCount() != 3 {
		t.Fatalf("later retry = %d, issuer calls %d, want a new attempt", reply.code, h.issuer.callCount())
	}
}

// Once a token is confirmed failed it is not handed out again, from memory or
// Redis, even if the replacement could not be stored; a later request retries.
func TestFetchConfirmedFailedTokenIsNotServedWhileRecovering(t *testing.T) {
	eachFetchProvider(t, func(t *testing.T, h *fetchHarness) {
		h.issue("token-a")
		wantToken(t, h.fetchBody(`{}`), "token-a", "issued")
		h.issue("token-b")
		h.store.setErr = context.DeadlineExceeded // the broker issues B, publishing fails
		body := `{"failed_token_sha256":"` + shaHex("token-a") + `"}`
		if reply := h.fetchBody(body); reply.code != http.StatusServiceUnavailable {
			t.Fatalf("report reply = %d", reply.code)
		}
		for range 2 {
			reply := h.fetchBody(`{}`)
			if reply.code != http.StatusServiceUnavailable || strings.Contains(reply.raw, "token-a") {
				t.Fatalf("ordinary fetch while recovering = %d %q", reply.code, reply.raw)
			}
		}
		h.store.setErr = nil
		wantToken(t, h.fetchBody(`{}`), "token-b", "issued")
		wantToken(t, h.fetchBody(`{}`), "token-b", h.reuseSource())
	})
}

// An approval key a peer published must itself be usable before the forced
// reissue yields to it.
func TestFetchForcedApprovalReissueDoesNotYieldToANearlyExpiredPeerKey(t *testing.T) {
	for _, provider := range []TokenProvider{ProviderKISMockApproval, ProviderKISLiveApproval} {
		t.Run(string(provider), func(t *testing.T) {
			h := newFetchHarness(t, provider)
			h.seed("rejected-key")
			wantToken(t, h.fetchBody(`{}`), "rejected-key", "redis")
			h.issue("fresh-key")
			var once sync.Once
			h.store.setNXHook = func() {
				once.Do(func() { h.store.save(h.ensurer.tokenKey(), "peer-short-lived", 30*time.Second) })
			}
			reply := h.fetchBody(`{"failed_token_sha256":"` + shaHex("rejected-key") + `"}`)
			wantToken(t, reply, "fresh-key", "issued")
		})
	}
}

func TestFetchTossReadsTokenRotatedByEnsure(t *testing.T) {
	h := newFetchHarness(t, ProviderToss)
	h.issue("before-rotation")
	wantToken(t, h.fetchBody(`{}`), "before-rotation", "issued")

	h.clock.advance(3600*time.Second - 90*time.Second)
	h.issue("after-rotation")
	response := httptest.NewRecorder()
	h.handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/toss/ensure", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"state":"issued"}` {
		t.Fatalf("ensure = %d %s, want issued", response.Code, response.Body)
	}
	wantToken(t, h.fetchBody(`{}`), "after-rotation", "redis")
	if h.issuer.callCount() != 2 {
		t.Fatalf("issuer calls = %d, want initial issue and one rotation", h.issuer.callCount())
	}
}

type blockingPanicSource struct {
	fetchSource
	started, release chan struct{}
}

func (source blockingPanicSource) EnsureReplacing(context.Context, string) (EnsureState, error) {
	close(source.started)
	<-source.release
	panic("synthetic issuer panic")
}

type observedDoneContext struct {
	context.Context
	waiting chan struct{}
}

func (ctx observedDoneContext) Done() <-chan struct{} {
	close(ctx.waiting)
	return ctx.Context.Done()
}

func TestFetchPanicFailsLeaderAndFollower(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.seed("synthetic-rejected")
	state := h.fetch.providers[h.provider]
	source := blockingPanicSource{state.source, make(chan struct{}), make(chan struct{})}
	state.source = source
	body := `{"failed_token_sha256":"` + shaHex("synthetic-rejected") + `"}`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	replies := make(chan int, 2)
	post := func(ctx context.Context) {
		request := httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/fetch", strings.NewReader(body)).WithContext(ctx)
		request.Header.Set("Authorization", "Bearer "+testFetchCredential)
		response := httptest.NewRecorder()
		h.handler.ServeHTTP(response, request)
		replies <- response.Code
	}
	go post(ctx)
	select {
	case <-source.started:
	case <-ctx.Done():
		close(source.release)
		t.Fatal("leader did not reach issuer")
	}
	waiting := make(chan struct{})
	go post(observedDoneContext{ctx, waiting})
	select {
	case <-waiting:
	case <-ctx.Done():
		close(source.release)
		t.Fatal("follower did not join")
	}
	close(source.release)
	for range 2 {
		if status := <-replies; status != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", status)
		}
	}
}

func TestFetchRecordsRejectedTokenFoundAfterEnsure(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	h.issue("synthetic-rejected")
	body := `{"failed_token_sha256":"` + shaHex("synthetic-rejected") + `"}`
	if status := h.fetchBody(body).code; status != http.StatusServiceUnavailable {
		t.Fatalf("failure report status = %d, want 503", status)
	}
	if status := h.fetchBody(`{}`).code; status != http.StatusServiceUnavailable {
		t.Fatalf("ordinary fetch status = %d, want 503", status)
	}
}

func TestFetchOrdinaryLeaderChecksRedisForSharedDigest(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	state := h.fetch.providers[h.provider]
	rejected := shaHex("synthetic-rejected")
	state.reject(rejected)
	state.issue <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := make(chan string, 2)
	start := func(digest string) {
		waiting := make(chan struct{})
		go func() {
			result, err := h.fetch.fetch(observedDoneContext{ctx, waiting}, h.provider, digest)
			if err != nil {
				results <- ""
				return
			}
			results <- result.SHA256
		}()
		select {
		case <-waiting:
		case <-ctx.Done():
			t.Fatal("request did not reach wait point")
		}
	}
	start("")
	start(rejected)
	h.seed("synthetic-redis")
	// A previous refresh completed while the ordinary leader was waiting.
	state.remember(&fetchEntry{sha256: shaHex("synthetic-memory"), expiresAt: h.clock.Now().Add(time.Hour)})
	<-state.issue
	for range 2 {
		if <-results != shaHex("synthetic-redis") {
			t.Error("shared attempt did not return the Redis token")
		}
	}
}

type delayedTTLStore struct{ clock *testClock }

func (store delayedTTLStore) ReadWithTTL(context.Context, string) (string, time.Duration, bool, error) {
	store.clock.advance(2 * time.Second)
	return "synthetic-approval", time.Second, true, nil
}

func TestFetchSnapshotAccountsForReadDelay(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMockApproval)
	h.fetch.store = delayedTTLStore{h.clock}
	entry, err := h.fetch.snapshot(t.Context(), h.provider, h.ensurer.tokenKey())
	if err != nil || entry != nil {
		t.Fatal("snapshot accepted a key whose TTL elapsed during the read")
	}
}

func TestRESPRejectsNestedArrayBeforeReadingItsLength(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*2\r\n*1\r\n$1\r\nx\r\n:1\r\n"))
	if _, err := readRESPReply(reader); err == nil {
		t.Fatal("accepted nested array")
	}
	if next, err := reader.ReadByte(); err != nil || next != '1' {
		t.Fatal("read beyond nested array marker")
	}
}

type fetchDeadlineWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (writer *fetchDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	writer.deadlines = append(writer.deadlines, deadline)
	return nil
}

type fetchTimeoutBody struct{ writer *fetchDeadlineWriter }

func (body fetchTimeoutBody) Read([]byte) (int, error) {
	if len(body.writer.deadlines) == 1 && body.writer.deadlines[0].After(time.Now()) {
		return 0, context.DeadlineExceeded
	}
	return 0, errors.New("read without deadline")
}

func TestFetchBodyReadSetsAndClearsRouteDeadline(t *testing.T) {
	h := newFetchHarness(t, ProviderKISMock)
	writer := &fetchDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	body := fetchTimeoutBody{writer}
	request := httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/fetch", io.NopCloser(body))
	request.Header.Set("Authorization", "Bearer "+testFetchCredential)
	h.handler.ServeHTTP(writer, request)
	if writer.Code != http.StatusBadRequest || len(writer.deadlines) != 2 ||
		writer.deadlines[0].IsZero() || !writer.deadlines[1].IsZero() || h.store.total() != 0 {
		t.Fatal("body timeout did not return 400 and clear the read deadline before issuer access")
	}
}
