package gatewayd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisClientUsesGETSetNXEXAndOwnedRelease(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	commands := make(chan []string, 5)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(commands)
		for range 5 {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			reader := bufio.NewReader(connection)
			writer := bufio.NewWriter(connection)
			arguments, readErr := readTestRESPArray(reader)
			if readErr == nil {
				commands <- arguments
				switch arguments[0] {
				case "GET":
					_, _ = writer.WriteString("$-1\r\n")
				case "SET":
					_, _ = writer.WriteString("+OK\r\n")
				case "PTTL":
					_, _ = writer.WriteString(":61000\r\n")
				case "EVAL":
					_, _ = writer.WriteString(":1\r\n")
				default:
					_, _ = writer.WriteString("-ERR unsupported\r\n")
				}
				_ = writer.Flush()
			}
			_ = connection.Close()
		}
	}()

	client, err := NewRedisClient("redis://" + listener.Addr().String() + "/0")
	if err != nil {
		t.Fatal(err)
	}
	if _, present, err := client.Get(context.Background(), "cache-key"); err != nil || present {
		t.Fatalf("GET = present %t, err %v", present, err)
	}
	if err := client.Set(context.Background(), "cache-key", "fixed-json-value", 3660*time.Second); err != nil {
		t.Fatalf("SET: %v", err)
	}
	locked, err := client.SetNX(context.Background(), "lock-key", "opaque-lock-value", 30*time.Second)
	if err != nil || !locked {
		t.Fatalf("SET NX = %t, %v", locked, err)
	}
	ttl, err := client.TTL(context.Background(), "cache-key")
	if err != nil || ttl != 61*time.Second {
		t.Fatalf("PTTL = %v, %v", ttl, err)
	}
	if err := client.CompareAndDelete(context.Background(), "lock-key", "opaque-lock-value"); err != nil {
		t.Fatalf("owned release: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fake Redis did not finish")
	}

	got := make([][]string, 0, 5)
	for command := range commands {
		got = append(got, command)
	}
	if len(got) != 5 {
		t.Fatalf("Redis commands = %d, want 5", len(got))
	}
	if !sameStrings(got[0], []string{"GET", "cache-key"}) {
		t.Fatalf("GET command = %q", got[0])
	}
	if !sameStrings(got[1], []string{"SET", "cache-key", "fixed-json-value", "EX", "3660"}) {
		t.Fatalf("cache SET command = %q", got[1])
	}
	if !sameStrings(got[2], []string{"SET", "lock-key", "opaque-lock-value", "NX", "EX", "30"}) {
		t.Fatalf("lock SET command = %q", got[2])
	}
	if !sameStrings(got[3], []string{"PTTL", "cache-key"}) {
		t.Fatalf("PTTL command = %q", got[3])
	}
	if len(got[4]) != 5 || got[4][0] != "EVAL" || got[4][2] != "1" || got[4][3] != "lock-key" || got[4][4] != "opaque-lock-value" {
		t.Fatalf("release command = %q", got[4])
	}
}

func TestRedisClientHeldUsesEXISTS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	commands := make(chan []string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(commands)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		reader := bufio.NewReader(connection)
		writer := bufio.NewWriter(connection)
		arguments, readErr := readTestRESPArray(reader)
		if readErr == nil {
			commands <- arguments
			_, _ = writer.WriteString(":1\r\n")
			_ = writer.Flush()
		}
		_ = connection.Close()
	}()

	client, err := NewRedisClient("redis://" + listener.Addr().String() + "/0")
	if err != nil {
		t.Fatal(err)
	}
	held, err := client.Held(context.Background(), "lock-key")
	if err != nil || !held {
		t.Fatalf("Held = %t, %v", held, err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fake Redis did not finish")
	}
	command := <-commands
	if !sameStrings(command, []string{"EXISTS", "lock-key"}) {
		t.Fatalf("held command = %q", command)
	}
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
	if err != nil || count < 1 || count > 8 {
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
		if _, err := io.ReadFull(reader, payload); err != nil || !strings.HasSuffix(string(payload), "\r\n") {
			return nil, fmt.Errorf("bulk payload")
		}
		arguments = append(arguments, string(payload[:length]))
	}
	return arguments, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
