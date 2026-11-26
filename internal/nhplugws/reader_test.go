package nhplugws

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
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
	reads  [][]byte
	writes [][]byte
}

func (s *fakeSocket) Read(context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) == 0 {
		return websocket.MessageText, nil, errors.New("fixture complete")
	}
	raw := s.reads[0]
	s.reads = s.reads[1:]
	return websocket.MessageText, raw, nil
}
func (s *fakeSocket) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	s.writes = append(s.writes, append([]byte(nil), data...))
	return nil
}
func (*fakeSocket) Close(websocket.StatusCode, string) error { return nil }

func TestReaderRecordsDocumentedFramesAndCountsMalformed(t *testing.T) {
	ack, err := os.ReadFile("testdata/subscription_ack.json")
	if err != nil {
		t.Fatal(err)
	}
	socket := &fakeSocket{reads: [][]byte{[]byte("not-json"), ack}}
	var recorded []Record
	wantTime := time.Date(2026, 11, 26, 1, 2, 3, 0, time.FixedZone("KST", 9*60*60))
	reader, err := New(Config{
		Endpoint: MockEndpoint,
		UserID:   "user-1",
		Quotes:   []Registration{{TRCD: "mc", TRKey: "005930"}},
		Token:    func(context.Context) (string, error) { return "fixture-token", nil },
		Append:   func(record Record) error { recorded = append(recorded, record); return nil },
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
	if err := reader.session(context.Background()); err == nil || !strings.Contains(err.Error(), "fixture complete") {
		t.Fatalf("session error = %v", err)
	}
	if got := reader.Status().MalformedFrames; got != 1 {
		t.Fatalf("malformed frames = %d", got)
	}
	if len(recorded) != 1 || recorded[0].TRCD != "mc" || !recorded[0].ReceivedAt.Equal(wantTime.UTC()) {
		t.Fatalf("recorded frames = %#v", recorded)
	}
	if string(recorded[0].TRKey) != `["005930"]` {
		t.Fatalf("record tr_key = %s", recorded[0].TRKey)
	}
	if len(socket.writes) != 5 {
		t.Fatalf("registration sends = %d, want 4 notices plus quote", len(socket.writes))
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
