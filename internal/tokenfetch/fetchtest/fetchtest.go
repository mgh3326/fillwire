// Package fetchtest is a fake gatewayd /fetch server that implements the
// wire contract, for tests of its consumers.
package fetchtest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// Clock is a test clock shared between the fake server and the consumer
// under test: real time plus a manual offset.
type Clock struct {
	mu  sync.Mutex
	adv time.Duration
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.adv)
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.adv += d
	c.mu.Unlock()
}

// Call is one request the server saw.
type Call struct {
	Provider string
	Failed   string // failed_token_sha256, "" when absent
	Auth     string // Authorization header
	Body     string
}

type state struct {
	token   string
	expires time.Time
}

// Server answers POST /v1/tokens/{provider}/fetch per the fixed contract.
type Server struct {
	*httptest.Server
	Clock *Clock
	// Credential is the accepted bearer credential.
	Credential string
	// TTL is the life of a freshly issued token (default 24h).
	TTL time.Duration

	mu        sync.Mutex
	known     map[string]bool
	forbidden map[string]bool
	states    map[string]*state
	issued    int
	calls     []Call
	status    int    // when non-zero, every authenticated request gets it
	errBody   string // body used with status; defaults to the contract code
}

var closedSet = []string{"kis-mock", "kis-live", "toss", "kis-live-approval", "kis-mock-approval"}

// New starts a server. Close it with Close.
func New(clock *Clock, credential string) *Server {
	s := &Server{Clock: clock, Credential: credential, TTL: 24 * time.Hour,
		known: map[string]bool{}, forbidden: map[string]bool{}, states: map[string]*state{}}
	for _, p := range closedSet {
		s.known[p] = true
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// Forbid makes the credential not allowed for provider (403).
func (s *Server) Forbid(provider string) { s.mu.Lock(); s.forbidden[provider] = true; s.mu.Unlock() }

// FailWith makes every authenticated request answer status (0 clears). body
// is sent verbatim when non-empty; use it to prove the client never echoes a
// response body.
func (s *Server) FailWith(status int, body string) {
	s.mu.Lock()
	s.status, s.errBody = status, body
	s.mu.Unlock()
}

// Seed installs a current token for provider without counting an issue.
func (s *Server) Seed(provider, token string, expires time.Time) {
	s.mu.Lock()
	s.states[provider] = &state{token: token, expires: expires}
	s.mu.Unlock()
}

// Current returns the server's current token for provider.
func (s *Server) Current(provider string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.states[provider]; st != nil {
		return st.token
	}
	return ""
}

// Calls returns a copy of every request seen so far.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// Issued counts tokens the server minted.
func (s *Server) Issued() int { s.mu.Lock(); defer s.mu.Unlock(); return s.issued }

func hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func errReply(w http.ResponseWriter, status int, code string) {
	reply(w, status, map[string]string{"error": code})
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/tokens/")
	provider, ok := strings.CutSuffix(path, "/fetch")
	if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/v1/tokens/") || !ok || provider == "" {
		http.NotFound(w, r)
		return
	}
	var raw strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		raw.Write(buf[:n])
		if err != nil {
			break
		}
	}
	call := Call{Provider: provider, Auth: r.Header.Get("Authorization"), Body: raw.String()}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := func() { s.calls = append(s.calls, call) }
	if call.Auth != "Bearer "+s.Credential || s.Credential == "" {
		record()
		errReply(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	if !s.known[provider] {
		record()
		errReply(w, http.StatusNotFound, "unknown_provider")
		return
	}
	if s.forbidden[provider] {
		record()
		errReply(w, http.StatusForbidden, "provider_not_allowed")
		return
	}
	var body struct {
		Failed *string `json:"failed_token_sha256"`
	}
	dec := json.NewDecoder(strings.NewReader(call.Body))
	dec.DisallowUnknownFields()
	if dec.Decode(&body) != nil || (body.Failed != nil && len(*body.Failed) != 64) {
		record()
		errReply(w, http.StatusBadRequest, "bad_request")
		return
	}
	if body.Failed != nil {
		call.Failed = *body.Failed
	}
	record()
	if s.status != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status)
		if s.errBody != "" {
			_, _ = w.Write([]byte(s.errBody))
		} else {
			_, _ = w.Write([]byte(`{"error":"issue_unavailable"}`))
		}
		return
	}
	now := s.Clock.Now()
	st := s.states[provider]
	source := "memory"
	issue := st == nil || !st.expires.After(now)
	if !issue {
		if call.Failed == "" {
			issue = st.expires.Sub(now) <= 60*time.Second
		} else {
			issue = hash(st.token) == call.Failed
		}
	}
	if issue {
		s.issued++
		st = &state{token: fmt.Sprintf("fake-%s-token-%d", provider, s.issued), expires: now.Add(s.TTL)}
		s.states[provider] = st
		source = "issued"
	}
	reply(w, http.StatusOK, map[string]string{
		"provider":     provider,
		"token":        st.token,
		"expires_at":   st.expires.UTC().Format(time.RFC3339),
		"token_sha256": hash(st.token),
		"source":       source,
	})
}
