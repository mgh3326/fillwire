package nhplugws

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestEndpointPolicyAndRegistrationList(t *testing.T) {
	var dialCount int
	for _, endpoint := range []string{
		"wss://moapi.nhplug.com:7070/websocket",
		"wss://moapi.nhplug.com:7080/websocket",
		"wss://api.nhplug.com:17070/websocket",
		"wss://token@moapi.nhplug.com:17070/websocket",
	} {
		_, err := New(Config{Endpoint: endpoint, UserID: "user", Token: func(context.Context) (string, error) { return "token", nil }, Append: func(Record) error { return nil }, Dial: func(context.Context, string) (Socket, error) { dialCount++; return nil, nil }})
		if err == nil {
			t.Fatalf("live endpoint %q accepted", endpoint)
		}
	}
	reader, err := New(Config{Endpoint: MockEndpoint, UserID: "user", Token: func(context.Context) (string, error) { return "token", nil }, Append: func(Record) error { return nil }, Dial: func(context.Context, string) (Socket, error) { dialCount++; return nil, nil }})
	if err != nil || reader == nil {
		t.Fatalf("mock endpoint rejected: %v", err)
	}
	if dialCount != 0 {
		t.Fatalf("validation dialed %d times", dialCount)
	}
	registrations, err := BuildRegistrations("user-1", []Registration{{TRCD: "mc", TRKey: "005930"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, channel := range []string{"d0", "d1", "d2", "d3"} {
		if registrations[i] != (Registration{TRCD: channel, TRKey: "user-1"}) {
			t.Fatalf("registration[%d] = %#v", i, registrations[i])
		}
	}
	if len(registrations) != 5 {
		t.Fatalf("registration count = %d", len(registrations))
	}
	if _, err := BuildRegistrations("user-1", make([]Registration, 27)); !errors.Is(err, ErrRegistrationLimit) {
		t.Fatalf("expected over-cap rejection, got %v", err)
	}
	exactCap := make([]Registration, 26)
	for i := range exactCap {
		exactCap[i] = Registration{TRCD: "mc", TRKey: "005930"}
	}
	if got, err := BuildRegistrations("user-1", exactCap); err != nil || len(got) != 30 {
		t.Fatalf("exact-cap registrations = %d, %v", len(got), err)
	}
	if _, err := BuildRegistrations("user-1", []Registration{{TRCD: "d0", TRKey: "user-1"}}); err == nil {
		t.Fatal("notification channel was accepted as a quote registration")
	}
}

type fakeSocket struct {
	mu               sync.Mutex
	reads            [][]byte
	writes           [][]byte
	writeDeadlines   int
	wantWrites       int
	writeSignal      chan struct{}
	closeAfterWrites bool
	ping             func(context.Context) error
}

func (s *fakeSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	for {
		s.mu.Lock()
		if len(s.reads) > 0 {
			raw := s.reads[0]
			s.reads = s.reads[1:]
			s.mu.Unlock()
			return websocket.MessageText, raw, nil
		}
		if s.closeAfterWrites && len(s.writes) >= s.wantWrites {
			s.mu.Unlock()
			return websocket.MessageText, nil, errors.New("fixture complete")
		}
		signal := s.writeSignal
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return websocket.MessageText, nil, ctx.Err()
		case <-signal:
		}
	}
}
func (s *fakeSocket) Write(ctx context.Context, _ websocket.MessageType, data []byte) error {
	s.mu.Lock()
	s.writes = append(s.writes, append([]byte(nil), data...))
	if _, ok := ctx.Deadline(); ok {
		s.writeDeadlines++
	}
	s.mu.Unlock()
	select {
	case s.writeSignal <- struct{}{}:
	default:
	}
	return nil
}
func (s *fakeSocket) Ping(ctx context.Context) error {
	if s.ping != nil {
		return s.ping(ctx)
	}
	return nil
}
func (*fakeSocket) Close(websocket.StatusCode, string) error { return nil }

func TestReaderRecordsDocumentedFramesAndCountsMalformed(t *testing.T) {
	ack, err := os.ReadFile("testdata/subscription_ack.json")
	if err != nil {
		t.Fatal(err)
	}
	socket := &fakeSocket{reads: [][]byte{[]byte("not-json"), ackFrame("d0", "user-1", "00000", "ok"), ackFrame("d1", "user-1", "00000", "ok"), ackFrame("d2", "user-1", "00000", "ok"), ackFrame("d3", "user-1", "00000", "ok"), ack}, wantWrites: 5, writeSignal: make(chan struct{}, 1), closeAfterWrites: true}
	var recorded []Record
	appendBeforeAllWrites := false
	wantTime := time.Date(2026, 11, 26, 1, 2, 3, 0, time.FixedZone("KST", 9*60*60))
	reader, err := New(Config{
		Endpoint: MockEndpoint,
		UserID:   "user-1",
		Quotes:   []Registration{{TRCD: "mc", TRKey: "005930"}},
		Token:    func(context.Context) (string, error) { return "fixture-token", nil },
		Append: func(record Record) error {
			socket.mu.Lock()
			if len(socket.writes) < socket.wantWrites {
				appendBeforeAllWrites = true
			}
			socket.mu.Unlock()
			recorded = append(recorded, record)
			return nil
		},
		Dial: func(_ context.Context, endpoint string) (Socket, error) {
			if endpoint != MockEndpoint {
				t.Fatalf("dialed %q", endpoint)
			}
			return socket, nil
		},
		Now: func() time.Time { return wantTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.session(context.Background()); err == nil || !strings.Contains(err.Error(), "fixture complete") {
		t.Fatalf("session error = %v", err)
	}
	if got := reader.Status().MalformedFrames; got != 1 {
		t.Fatalf("malformed frames = %d", got)
	}
	if len(recorded) != 5 || recorded[0].TRCD != "d0" || !recorded[0].ReceivedAt.Equal(wantTime.UTC()) {
		t.Fatalf("recorded frames = %#v", recorded)
	}
	if !appendBeforeAllWrites {
		t.Fatal("frames were not read and recorded while registrations were being paced")
	}
	if string(recorded[len(recorded)-1].TRKey) != `["005930"]` {
		t.Fatalf("record tr_key = %s", recorded[len(recorded)-1].TRKey)
	}
	socket.mu.Lock()
	defer socket.mu.Unlock()
	if len(socket.writes) != 5 {
		t.Fatalf("registration sends = %d, want 4 notices plus quote", len(socket.writes))
	}
	if socket.writeDeadlines != len(socket.writes) {
		t.Fatalf("registration writes with deadlines = %d, want %d", socket.writeDeadlines, len(socket.writes))
	}
	var sent struct {
		Header struct {
			Token string `json:"token"`
			Type  string `json:"tr_type"`
		} `json:"header"`
		Body Registration `json:"body"`
	}
	if err := json.Unmarshal(socket.writes[0], &sent); err != nil || sent.Header.Token != "fixture-token" || sent.Header.Type != "1" || sent.Body.TRCD != "d0" || sent.Body.TRKey != "user-1" {
		t.Fatalf("first registration = %s (%v)", socket.writes[0], err)
	}
}

func ackFrame(channel, key, code, message string) []byte {
	frame, _ := json.Marshal(map[string]any{
		"header": map[string]string{"tr_type": "1", "tr_cd": channel, "rsp_cd": code, "rsp_msg": message},
		"body":   map[string][]string{"tr_key": {key}},
	})
	return frame
}

func TestHandshakeDialHasDeadline(t *testing.T) {
	var gotDeadline bool
	reader, err := New(Config{
		UserID: "user",
		Token:  func(context.Context) (string, error) { return "token", nil },
		Append: func(Record) error { return nil },
		Dial: func(ctx context.Context, _ string) (Socket, error) {
			_, gotDeadline = ctx.Deadline()
			return nil, context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = reader.session(context.Background())
	if !gotDeadline {
		t.Fatal("dial context has no handshake deadline")
	}
}

func TestRedirectPolicyRefusesEveryRedirect(t *testing.T) {
	client := noRedirectHTTPClient()
	request, _ := http.NewRequest(http.MethodGet, "wss://api.nhplug.com:7080/websocket", nil)
	if err := client.CheckRedirect(request, []*http.Request{}); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect policy error = %v", err)
	}
}

func TestRegistrationRejectionFailsAndInvalidatesRejectedToken(t *testing.T) {
	socket := &fakeSocket{reads: [][]byte{ackFrame("d0", "user", "WSS10001", "invalid token")}, wantWrites: 1, writeSignal: make(chan struct{}, 1), closeAfterWrites: true}
	invalidated := ""
	reader, err := New(Config{
		UserID:          "user",
		Token:           func(context.Context) (string, error) { return "bad-token", nil },
		InvalidateToken: func(token string) { invalidated = token },
		Append:          func(Record) error { return nil },
		Dial:            func(context.Context, string) (Socket, error) { return socket, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.session(context.Background())
	if !errors.Is(err, ErrRegistrationRejected) {
		t.Fatalf("rejection error = %v", err)
	}
	if invalidated != "bad-token" {
		t.Fatalf("invalidated token = %q", invalidated)
	}
}

func TestAppendFailureIsFatal(t *testing.T) {
	socket := &fakeSocket{reads: [][]byte{ackFrame("d0", "user", "00000", "ok")}, wantWrites: 1, writeSignal: make(chan struct{}, 1), closeAfterWrites: true}
	reader, err := New(Config{
		UserID: "user",
		Token:  func(context.Context) (string, error) { return "token", nil },
		Append: func(Record) error { return errors.New("disk unavailable") },
		Dial:   func(context.Context, string) (Socket, error) { return socket, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Run(context.Background()); !errors.Is(err, ErrAppend) {
		t.Fatalf("append error = %v", err)
	}
}

func TestPermanentOAuthCredentialFailureStopsReader(t *testing.T) {
	reader, err := New(Config{
		UserID: "user",
		Token: func(context.Context) (string, error) {
			return "", ErrOAuthCredentialsRejected
		},
		Append:   func(Record) error { return nil },
		RetryMin: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := reader.Run(ctx); !errors.Is(err, ErrOAuthCredentialsRejected) {
		t.Fatalf("reader error = %v", err)
	}
}

func TestNotificationAcknowledgementTimeoutAndPingFailureReconnect(t *testing.T) {
	t.Run("ack timeout", func(t *testing.T) {
		socket := &fakeSocket{writeSignal: make(chan struct{}, 1)}
		reader, err := New(Config{
			UserID: "user", AckTimeout: 10 * time.Millisecond,
			Token:  func(context.Context) (string, error) { return "token", nil },
			Append: func(Record) error { return nil },
			Dial:   func(context.Context, string) (Socket, error) { return socket, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.session(context.Background()); !errors.Is(err, ErrAckTimeout) {
			t.Fatalf("ack timeout error = %v", err)
		}
	})
	t.Run("ping failure", func(t *testing.T) {
		socket := &fakeSocket{writeSignal: make(chan struct{}, 1), wantWrites: 5}
		pingFailed := errors.New("peer stopped responding")
		socketPing := func(ctx context.Context) error { return pingFailed }
		socket.ping = socketPing
		reader, err := New(Config{
			UserID: "user", PingInterval: time.Millisecond,
			Token:  func(context.Context) (string, error) { return "token", nil },
			Append: func(Record) error { return nil },
			Dial:   func(context.Context, string) (Socket, error) { return socket, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.session(context.Background()); err == nil || !strings.Contains(err.Error(), "ping failed") {
			t.Fatalf("ping failure error = %v", err)
		}
	})
}

func TestRetryDelayResetsAfterHealthySession(t *testing.T) {
	if got := retryDelay(30*time.Second, 31*time.Second, time.Second, 30*time.Second); got != time.Second {
		t.Fatalf("healthy-session retry delay = %s", got)
	}
	if got := retryDelay(4*time.Second, time.Second, time.Second, 30*time.Second); got != 8*time.Second {
		t.Fatalf("short-session retry delay = %s", got)
	}
}

func TestValidateEndpointAcceptsOnlyMockPort(t *testing.T) {
	if err := ValidateEndpoint(MockEndpoint); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"wss://moapi.nhplug.com:7070/websocket", "wss://moapi.nhplug.com:7080/websocket"} {
		if err := ValidateEndpoint(endpoint); err == nil {
			t.Fatalf("accepted %q", endpoint)
		}
	}
}

func TestRunAllowsOnlyOneActiveSession(t *testing.T) {
	started := make(chan struct{})
	reader, err := New(Config{
		UserID: "user-1",
		Token: func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		},
		Append: func(Record) error { return nil },
		Dial: func(context.Context, string) (Socket, error) {
			t.Fatal("canceled token lookup must not dial")
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reader.Run(ctx) }()
	<-started
	if err := reader.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run error = %v", err)
	}
}
