package gatewayd

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// fetchFreshBuffer is how much life a token must have left to be served from
// memory. It matches the 60 s margin used by the existing ensure path.
const fetchFreshBuffer = 60 * time.Second

const (
	fetchSourceMemory = "memory"
	fetchSourceRedis  = "redis"
	fetchSourceIssued = "issued"

	fetchBodyLimit = 1024
)

var (
	errFetchUnavailable = errors.New("token fetch unavailable")
	failedHashPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// TokenReadStore reads a cache value together with its remaining TTL in one
// atomic step. *RedisClient implements it with a single script.
type TokenReadStore interface {
	ReadWithTTL(ctx context.Context, key string) (value string, ttl time.Duration, present bool, err error)
}

// fetchSource is the issuance capability /fetch composes: the same Ensure the
// /ensure route calls, its force variant, and the Redis key it publishes to.
type fetchSource interface {
	Ensurer
	EnsureReplacing(ctx context.Context, rejectedSHA string) (EnsureState, error)
	tokenKey() string
}

type fetchEntry struct {
	token     string
	sha256    string
	expiresAt time.Time
}

func (entry *fetchEntry) usable(now time.Time, buffer time.Duration) bool {
	return entry != nil && entry.expiresAt.Sub(now) > buffer
}

type fetchProvider struct {
	source fetchSource
	// issue serializes the miss path per provider. It is a channel so a waiting
	// request still honors its context.
	issue chan struct{}

	mu    sync.RWMutex
	entry *fetchEntry
	// rejected is the hash of a token a consumer reported failed and that is
	// still current. It is never served (memory or Redis) until a different
	// usable token replaces it, so a failed replacement cannot leave callers
	// holding the dead token.
	rejected string

	attemptMu sync.Mutex
	attempts  map[string]*fetchAttempt
}

// fetchAttempt is one in-flight replacement of a rejected token. Overlapping
// requests for the same rejected hash share its result, including a failure.
type fetchAttempt struct {
	done   chan struct{}
	result FetchResult
	err    error
}

func (provider *fetchProvider) current() *fetchEntry {
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return provider.entry
}

func (provider *fetchProvider) rejectedSHA() string {
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return provider.rejected
}

// remember stores the current token; a different token ends the rejection.
func (provider *fetchProvider) remember(entry *fetchEntry) {
	provider.mu.Lock()
	provider.entry = entry
	if provider.rejected != "" && entry.sha256 != provider.rejected {
		provider.rejected = ""
	}
	provider.mu.Unlock()
}

// reject marks a confirmed-failed token and drops it from memory.
func (provider *fetchProvider) reject(sha string) {
	provider.mu.Lock()
	provider.rejected = sha
	if provider.entry != nil && provider.entry.sha256 == sha {
		provider.entry = nil
	}
	provider.mu.Unlock()
}

// join registers the caller as leader of the attempt for a rejected hash, or
// returns the attempt already running for it.
func (provider *fetchProvider) join(sha string) (*fetchAttempt, bool) {
	provider.attemptMu.Lock()
	defer provider.attemptMu.Unlock()
	if attempt, running := provider.attempts[sha]; running {
		return attempt, false
	}
	attempt := &fetchAttempt{done: make(chan struct{})}
	provider.attempts[sha] = attempt
	return attempt, true
}

func (provider *fetchProvider) finish(sha string, attempt *fetchAttempt) {
	provider.attemptMu.Lock()
	delete(provider.attempts, sha)
	provider.attemptMu.Unlock()
	close(attempt.done)
}

type fetchConsumer struct {
	digest    [sha256.Size]byte
	providers map[TokenProvider]struct{}
}

// FetchResult is the 200 body of /fetch. Callers must not log or format it.
type FetchResult struct {
	Provider  TokenProvider `json:"provider"`
	Token     string        `json:"token"`
	ExpiresAt string        `json:"expires_at"`
	SHA256    string        `json:"token_sha256"`
	Source    string        `json:"source"`
}

// FetchService hands tokens to authorized local consumers. Assumption: this
// gatewayd is the sole issuer for each credential, so a token another issuer
// rotates is noticed only through a consumer's failed_token_sha256 report.
type FetchService struct {
	store     TokenReadStore
	providers map[TokenProvider]*fetchProvider
	consumers []fetchConsumer
	now       func() time.Time
}

// NewFetchService composes /fetch over the configured ensurers. Providers whose
// ensurer cannot force or name its Redis key are not fetchable. A nil store or
// an empty consumer list is valid: the former fails every miss, the latter
// rejects every request with 401.
func NewFetchService(ensurers ProviderEnsurers, store TokenReadStore, consumers []FetchConsumer) *FetchService {
	service := &FetchService{
		store:     store,
		providers: make(map[TokenProvider]*fetchProvider, len(ensurers)),
		now:       time.Now,
	}
	for provider, ensurer := range ensurers {
		if source, ok := ensurer.(fetchSource); ok && knownProvider(provider) {
			service.providers[provider] = &fetchProvider{source: source, issue: make(chan struct{}, 1), attempts: make(map[string]*fetchAttempt)}
		}
	}
	for _, consumer := range consumers {
		allowed := make(map[TokenProvider]struct{}, len(consumer.Providers))
		for _, provider := range consumer.Providers {
			allowed[provider] = struct{}{}
		}
		service.consumers = append(service.consumers, fetchConsumer{
			digest:    sha256.Sum256([]byte(consumer.Credential)),
			providers: allowed,
		})
	}
	return service
}

// authenticate returns the consumer matching the Authorization header. Every
// configured credential is compared (digest, constant time) with no early exit.
func (service *FetchService) authenticate(header string) *fetchConsumer {
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || credential == "" || strings.Contains(credential, " ") {
		return nil
	}
	digest := sha256.Sum256([]byte(credential))
	var matched *fetchConsumer
	for index := range service.consumers {
		if subtle.ConstantTimeCompare(digest[:], service.consumers[index].digest[:]) == 1 {
			matched = &service.consumers[index]
		}
	}
	return matched
}

// snapshot reads the provider's Redis value and TTL atomically. A missing,
// malformed or expired value is (nil, nil); only a store failure is an error.
func (service *FetchService) snapshot(ctx context.Context, provider TokenProvider, key string) (*fetchEntry, error) {
	if service.store == nil || key == "" {
		return nil, errFetchUnavailable
	}
	raw, ttl, present, err := service.store.ReadWithTTL(ctx, key)
	if err != nil {
		return nil, errFetchUnavailable
	}
	if !present || ttl <= 0 {
		return nil, nil
	}
	now := service.now()
	token, payloadExpiry, ok := parseCachedToken(provider, raw)
	if !ok {
		return nil, nil
	}
	expiresAt := now.Add(ttl)
	if !payloadExpiry.IsZero() && payloadExpiry.Before(expiresAt) {
		expiresAt = payloadExpiry
	}
	if !expiresAt.After(now) {
		return nil, nil
	}
	return &fetchEntry{token: token, sha256: tokenSHA256(token), expiresAt: expiresAt}, nil
}

// parseCachedToken extracts the token from the provider's Redis value. JSON
// providers also report the payload's own expiry; the approval value is raw.
func parseCachedToken(provider TokenProvider, raw string) (string, time.Time, bool) {
	if isApprovalProvider(provider) {
		if strings.TrimSpace(raw) == "" || !safeHeaderText(raw) {
			return "", time.Time{}, false
		}
		return raw, time.Time{}, true
	}
	fields, valid := strictJSONFields(raw)
	if !valid {
		return "", time.Time{}, false
	}
	for key := range fields {
		switch key {
		case "access_token", "expires_at":
		case "created_at":
			if provider == ProviderToss {
				return "", time.Time{}, false
			}
		default:
			return "", time.Time{}, false
		}
	}
	var token string
	var expiresAt float64
	tokenRaw, hasToken := fields["access_token"]
	expiresRaw, hasExpiry := fields["expires_at"]
	if !hasToken || !hasExpiry || json.Unmarshal(tokenRaw, &token) != nil || json.Unmarshal(expiresRaw, &expiresAt) != nil ||
		token == "" || !safeHeaderText(token) || math.IsNaN(expiresAt) || expiresAt > 1e12 || expiresAt < 0 {
		return "", time.Time{}, false
	}
	seconds := int64(expiresAt)
	return token, time.Unix(seconds, int64((expiresAt-float64(seconds))*float64(time.Second))), true
}

func tokenSHA256(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (entry *fetchEntry) result(provider TokenProvider, source string) FetchResult {
	return FetchResult{
		Provider:  provider,
		Token:     entry.token,
		ExpiresAt: entry.expiresAt.UTC().Truncate(time.Second).Format(time.RFC3339),
		SHA256:    entry.sha256,
		Source:    source,
	}
}

// fetch implements the /fetch semantics for a configured provider. failedSHA
// is empty or a validated lowercase hex digest.
func (service *FetchService) fetch(ctx context.Context, provider TokenProvider, failedSHA string) (FetchResult, error) {
	state, found := service.providers[provider]
	if !found {
		return FetchResult{}, errFetchUnavailable
	}
	// Only a request with no failure report may be answered from memory alone:
	// a report must be checked against Redis, which is the shared record.
	if failedSHA == "" {
		if result, hit := service.memoryHit(state, provider); hit {
			return result, nil
		}
	}
	replacing := failedSHA
	if replacing == "" {
		replacing = state.rejectedSHA()
	}
	if replacing == "" {
		return service.refresh(ctx, provider, state, failedSHA)
	}
	attempt, leader := state.join(replacing)
	if !leader {
		select {
		case <-attempt.done:
			return attempt.result, attempt.err
		case <-ctx.Done():
			return FetchResult{}, errFetchUnavailable
		}
	}
	defer state.finish(replacing, attempt)
	attempt.result, attempt.err = service.refresh(ctx, provider, state, failedSHA)
	return attempt.result, attempt.err
}

func (service *FetchService) memoryHit(state *fetchProvider, provider TokenProvider) (FetchResult, bool) {
	entry := state.current()
	if entry.usable(service.now(), fetchFreshBuffer) && entry.sha256 != state.rejectedSHA() {
		return entry.result(provider, fetchSourceMemory), true
	}
	return FetchResult{}, false
}

// refresh is the miss path, run by one request at a time per provider.
func (service *FetchService) refresh(ctx context.Context, provider TokenProvider, state *fetchProvider, failedSHA string) (FetchResult, error) {
	select {
	case state.issue <- struct{}{}:
		defer func() { <-state.issue }()
	case <-ctx.Done():
		return FetchResult{}, errFetchUnavailable
	}
	if failedSHA == "" {
		// Another request may have refreshed memory while this one waited.
		if result, hit := service.memoryHit(state, provider); hit {
			return result, nil
		}
	}

	// Redis is the shared record of the current token. A token is dead if the
	// caller reported it or an earlier report already condemned it; a caller
	// reporting some other token is stale and gets the current one.
	rejected := state.rejectedSHA()
	force := false
	currentSHA := ""
	if failedSHA != "" || rejected != "" {
		current, err := service.snapshot(ctx, provider, state.source.tokenKey())
		if err != nil {
			return FetchResult{}, err
		}
		if current != nil {
			if current.sha256 == failedSHA || current.sha256 == rejected {
				force, currentSHA = true, current.sha256
				state.reject(currentSHA)
			} else if current.usable(service.now(), fetchFreshBuffer) {
				state.remember(current)
				return current.result(provider, fetchSourceRedis), nil
			}
		}
	}

	var ensured EnsureState
	var err error
	if force {
		ensured, err = state.source.EnsureReplacing(ctx, currentSHA)
	} else {
		ensured, err = state.source.Ensure(ctx)
	}
	if err != nil || (ensured != EnsureStateFresh && ensured != EnsureStateIssued) {
		return FetchResult{}, errFetchUnavailable
	}
	current, err := service.snapshot(ctx, provider, state.source.tokenKey())
	if err != nil || current == nil {
		return FetchResult{}, errFetchUnavailable
	}
	// A token the consumer rejected is still current: reissue did not replace it
	// (contended lock, or the broker returned the same token). Report that
	// instead of looping, and never hand it out.
	if (failedSHA != "" && current.sha256 == failedSHA) || current.sha256 == state.rejectedSHA() {
		return FetchResult{}, errFetchUnavailable
	}
	state.remember(current)
	source := fetchSourceRedis
	if ensured == EnsureStateIssued {
		source = fetchSourceIssued
	}
	return current.result(provider, source), nil
}

// registerFetch adds POST /v1/tokens/{provider}/fetch. Logs and metrics carry
// fixed text and bounded labels only; no token, credential or request value.
func (service *FetchService) registerFetch(mux *http.ServeMux, logger EventLogger, metrics *Metrics) {
	mux.HandleFunc("POST /v1/tokens/{provider}/fetch", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		provider := TokenProvider(request.PathValue("provider"))
		defer func() {
			// The recovered value is dropped: a panic message could echo a token.
			if recover() != nil {
				logEvent(logger, "gatewayd: token fetch failed")
				metrics.recordFetch(provider, fetchResultFailed)
				writeFetchError(writer, http.StatusServiceUnavailable, "issue_unavailable")
			}
		}()
		consumer := service.authenticate(request.Header.Get("Authorization"))
		if consumer == nil {
			logEvent(logger, "gatewayd: token fetch unauthenticated")
			writer.Header().Set("WWW-Authenticate", "Bearer")
			writeFetchError(writer, http.StatusUnauthorized, "unauthenticated")
			return
		}
		if !knownProvider(provider) {
			logEvent(logger, "gatewayd: token fetch unknown provider")
			writeFetchError(writer, http.StatusNotFound, "unknown_provider")
			return
		}
		if _, allowed := consumer.providers[provider]; !allowed {
			logEvent(logger, "gatewayd: token fetch provider not allowed")
			metrics.recordFetch(provider, fetchResultDenied)
			writeFetchError(writer, http.StatusForbidden, "provider_not_allowed")
			return
		}
		if _, configured := service.providers[provider]; !configured {
			logEvent(logger, "gatewayd: token fetch unknown provider")
			metrics.recordFetch(provider, fetchResultDenied)
			writeFetchError(writer, http.StatusNotFound, "unknown_provider")
			return
		}
		failedSHA, ok := decodeFetchBody(writer, request)
		if !ok {
			logEvent(logger, "gatewayd: token fetch bad request")
			writeFetchError(writer, http.StatusBadRequest, "bad_request")
			return
		}
		result, err := service.fetch(request.Context(), provider, failedSHA)
		if err != nil {
			logEvent(logger, "gatewayd: token fetch failed")
			metrics.recordFetch(provider, fetchResultFailed)
			writeFetchError(writer, http.StatusServiceUnavailable, "issue_unavailable")
			return
		}
		body, err := json.Marshal(result)
		if err != nil {
			logEvent(logger, "gatewayd: token fetch failed")
			metrics.recordFetch(provider, fetchResultFailed)
			writeFetchError(writer, http.StatusServiceUnavailable, "issue_unavailable")
			return
		}
		logEvent(logger, "gatewayd: token fetch "+result.Source)
		metrics.recordFetch(provider, result.Source)
		writeJSON(writer, http.StatusOK, string(body))
	})
}

// decodeFetchBody accepts exactly one JSON object whose only field is an
// optional failed_token_sha256 of 64 lowercase hex characters.
func decodeFetchBody(writer http.ResponseWriter, request *http.Request) (string, bool) {
	raw, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, fetchBodyLimit))
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return "", false
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var body struct {
		Failed *string `json:"failed_token_sha256"`
	}
	if decoder.Decode(&body) != nil {
		return "", false
	}
	if _, tokenErr := decoder.Token(); tokenErr != io.EOF {
		return "", false
	}
	if body.Failed == nil {
		return "", true
	}
	if !failedHashPattern.MatchString(*body.Failed) {
		return "", false
	}
	return *body.Failed, true
}

func writeFetchError(writer http.ResponseWriter, status int, code string) {
	writeJSON(writer, status, `{"error":"`+code+`"}`)
}
