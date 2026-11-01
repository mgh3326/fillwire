// Command bookrec records KIS order-book and trade frames into append-only
// evidence files and replays them deterministically.
//
//	bookrec record -out FILE [-stale 2s] < frames
//	bookrec replay -in FILE [-out FILE]
//
// record reads raw KIS realtime frames, one per line, from stdin and stamps
// each with its receive time. replay writes one JSON event per line (books,
// trades, gaps, stale streams, reconnects), then the final book of every
// symbol, and prints a summary to stderr.
//
// bookrec has no network client and no order capability; noorder_test.go
// fails if order code or a non-allowlisted package is linked in.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, time.Now))
}

const usage = "usage: bookrec record -out FILE [-stale 2s] < frames\n       bookrec replay -in FILE [-out FILE]\n"

func run(args []string, stdin io.Reader, stdout, stderr io.Writer, now func() time.Time) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "record":
		err = record(args[1:], stdin, stderr, now)
	case "replay":
		err = replay(args[1:], stdout, stderr)
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "bookrec: %v\n", err)
		return 1
	}
	return 0
}

func record(args []string, stdin io.Reader, stderr io.Writer, now func() time.Time) error {
	flags := flag.NewFlagSet("record", flag.ContinueOnError)
	flags.SetOutput(stderr)
	out := flags.String("out", "", "evidence file to append to")
	stale := flags.Duration("stale", book.DefaultStaleAfter, "mark a stream stale after this much silence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("record: -out is required")
	}
	if *stale < time.Millisecond {
		return errors.New("record: -stale must be at least 1ms")
	}
	file, err := book.OpenEvidence(*out)
	if err != nil {
		return err
	}
	defer file.Close()
	rec, err := book.NewRecorder(file, *stale, now())
	if err != nil {
		return err
	}
	tick := time.NewTicker(*stale / 4)
	defer tick.Stop()
	if err := recordFrom(rec, stdin, tick.C, now); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

// recordFrom feeds stdin lines and ticks to rec from one goroutine, so a
// write failure from either ends the recording at once, even while stdin is
// idle.
func recordFrom(rec *book.Recorder, stdin io.Reader, tick <-chan time.Time, now func() time.Time) error {
	lines := make(chan []byte)
	readErr := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		scanner := bufio.NewScanner(stdin)
		scanner.Buffer(make([]byte, 64*1024), 1<<20)
		for scanner.Scan() {
			select {
			case lines <- bytes.Clone(scanner.Bytes()):
			case <-done:
				return
			}
		}
		readErr <- scanner.Err()
		close(lines)
	}()
	for {
		select {
		case line, open := <-lines:
			if !open {
				return <-readErr
			}
			if len(line) == 0 {
				continue
			}
			if err := rec.Frame(line, now()); err != nil {
				return err
			}
		case <-tick:
			if err := rec.Tick(now()); err != nil {
				return err
			}
		}
	}
}

func replay(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	in := flags.String("in", "", "evidence file to replay")
	outPath := flags.String("out", "", "write events here instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *in == "" {
		return errors.New("replay: -in is required")
	}
	src, err := os.Open(*in)
	if err != nil {
		return err
	}
	defer src.Close()
	if *outPath == "" {
		return writeReplay(src, stdout, stderr)
	}
	if err := refuseSameFile(src, *outPath); err != nil {
		return err
	}
	file, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := writeReplay(src, file, stderr); err != nil {
		return err
	}
	return file.Close()
}

// refuseSameFile stops -out from truncating the evidence being replayed,
// whatever path (or link) names it.
func refuseSameFile(src *os.File, outPath string) error {
	in, err := src.Stat()
	if err != nil {
		return err
	}
	out, err := os.Stat(outPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if os.SameFile(in, out) {
		return errors.New("replay: -out names the -in evidence file")
	}
	return nil
}

func writeReplay(src io.Reader, out, stderr io.Writer) error {
	buffered := bufio.NewWriter(out)
	enc := json.NewEncoder(buffered)
	state, err := book.Replay(src, func(e book.Event) error { return enc.Encode(e) })
	if err != nil {
		return err
	}
	for _, b := range state.SortedBooks() {
		if err := enc.Encode(b); err != nil {
			return err
		}
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	summary, _ := json.Marshal(state.Summary)
	fmt.Fprintf(stderr, "bookrec: replay summary %s\n", summary)
	return nil
}
