package kisfutures

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// A JSONL write cut short is removed before the bar is written again: every
// line of the file stays one whole row.
func TestPartialJSONLRowIsRemovedBeforeTheRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bars.jsonl")
	sink, err := NewJSONLSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	bar := Bar{TimeUTC: kstAt(9, 0, 0), Symbol: "F200.2612", Volume: 3, ReceivedAt: kstAt(9, 1, 10)}
	if err := sink.Write(context.Background(), bar); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// The file size limit cuts the second row after 20 bytes. Go ignores the
	// SIGXFSZ the kernel sends with the error.
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	short := limit
	short.Cur = uint64(info.Size()) + 20
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &short); err != nil {
		t.Fatal(err)
	}
	writeErr := sink.Write(context.Background(), bar)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	if writeErr == nil {
		t.Fatal("the cut write succeeded")
	}
	if err := sink.Write(context.Background(), bar); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %q", lines)
	}
	for _, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Errorf("invalid line %q", line)
		}
	}
}
