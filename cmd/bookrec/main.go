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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
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
	if *stale <= 0 {
		return errors.New("record: -stale must be positive")
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

	stop := make(chan struct{})
	var ticker sync.WaitGroup
	ticker.Add(1)
	go func() {
		defer ticker.Done()
		tick := time.NewTicker(*stale / 4)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if rec.Tick(now()) != nil {
					return
				}
			}
		}
	}()

	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		if err := rec.Frame(scanner.Bytes(), now()); err != nil {
			break
		}
	}
	close(stop)
	ticker.Wait()
	if err := rec.Err(); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return file.Sync()
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
	out := stdout
	if *outPath != "" {
		file, err := os.Create(*outPath)
		if err != nil {
			return err
		}
		defer file.Close()
		out = file
	}
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
