package kismockread

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisGETClientSendsOnlyGET(t *testing.T) {
	token := redactionProbeToken()
	redisURL, commands, finish := startFakeRedis(t, cachedTokenPayload(token, float64(testNow().Add(2*time.Hour).Unix())), true)
	client, err := NewRedisGETClient(redisURL)
	if err != nil {
		t.Fatalf("new redis getter: %v", err)
	}

	value, present, getErr := client.Get(context.Background(), "kis_mock:scope:access_token")
	finish()
	if getErr != nil || !present || value != cachedTokenPayload(token, float64(testNow().Add(2*time.Hour).Unix())) {
		t.Fatal("expected the fake Redis value to be returned")
	}
	got := drainCommands(commands)
	if len(got) != 1 || got[0] != "GET" {
		t.Fatalf("unexpected Redis command sequence: %v", got)
	}
}

func TestMissingOrExpiredTokenFailsClosedBeforeKIS(t *testing.T) {
	now := testNow()
	tests := []struct {
		name    string
		payload string
		present bool
		want    ErrorCode
	}{
		{name: "missing", present: false, want: CodeTokenMissing},
		{
			name:    "expired after buffer",
			payload: cachedTokenPayload(redactionProbeToken(), float64(now.Add(tokenExpiryBuffer).Unix())),
			present: true,
			want:    CodeTokenExpired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redisURL, commands, finish := startFakeRedis(t, test.payload, test.present)
			getter, err := NewRedisGETClient(redisURL)
			if err != nil {
				t.Fatalf("new redis getter: %v", err)
			}
			transport := &recordingTransport{
				respond: func(*http.Request) (*http.Response, error) {
					return jsonResponse(http.StatusOK, `{"rt_cd":"0","output1":[]}`, nil), nil
				},
			}
			_, executeErr := (Executor{
				TokenGetter: getter,
				Transport:   transport,
				Now:         func() time.Time { return now },
			}).Execute(context.Background(), testConfig(), ReadRequest{Operation: OperationDomesticBalance})
			finish()

			if executeErr == nil || executeErr.Code != test.want {
				t.Fatalf("want %s, got %v", test.want, executeErr)
			}
			if transport.calls != 0 {
				t.Fatalf("KIS transport called %d times after cache failure", transport.calls)
			}
			got := drainCommands(commands)
			if len(got) != 1 || got[0] != "GET" {
				t.Fatalf("unexpected Redis command sequence: %v", got)
			}
		})
	}
}

func startFakeRedis(t *testing.T, value string, present bool) (string, <-chan string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	commands := make(chan string, 4)
	done := make(chan struct{})
	go func() {
		defer close(commands)
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		writer := bufio.NewWriter(connection)
		arguments, readErr := readTestRESPArray(reader)
		if readErr != nil || len(arguments) == 0 {
			return
		}
		commands <- arguments[0]
		if arguments[0] != "GET" {
			_, _ = writer.WriteString("+OK\r\n")
			_ = writer.Flush()
			return
		}
		if !present {
			_, _ = writer.WriteString("$-1\r\n")
			_ = writer.Flush()
			return
		}
		_, _ = fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(value), value)
		_ = writer.Flush()
	}()

	finish := func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("fake Redis did not finish")
		}
	}
	return "redis://" + listener.Addr().String() + "/0", commands, finish
}

func drainCommands(commands <-chan string) []string {
	var output []string
	for command := range commands {
		output = append(output, command)
	}
	return output
}

func readTestRESPArray(reader *bufio.Reader) ([]string, error) {
	marker, err := reader.ReadByte()
	if err != nil || marker != '*' {
		return nil, fmt.Errorf("array marker")
	}
	countLine, err := readRESPLine(reader)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(countLine)
	if err != nil || count < 0 || count > 8 {
		return nil, fmt.Errorf("array length")
	}
	arguments := make([]string, 0, count)
	for range count {
		marker, err := reader.ReadByte()
		if err != nil || marker != '$' {
			return nil, fmt.Errorf("bulk marker")
		}
		lengthLine, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(lengthLine)
		if err != nil || length < 0 || length > redisReplyLimit {
			return nil, fmt.Errorf("bulk length")
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil ||
			!strings.HasSuffix(string(payload), "\r\n") {
			return nil, fmt.Errorf("bulk payload")
		}
		arguments = append(arguments, string(payload[:length]))
	}
	return arguments, nil
}
