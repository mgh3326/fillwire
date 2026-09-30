// Package quote streams KIS domestic trade prices (H0STCNT0) and best-level
// order book quotes (H0STASP0) into a Redis Stream.
//
// It is a separate lane from the execution-fill pipeline. It owns its own KIS
// app key, approval provider, websocket, Redis client, and goroutines, and it
// never places orders, applies policy, or writes to the execution ledger.
package quote

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
)

// MaxSymbols bounds the configured symbol list. A longer list is refused
// rather than truncated.
const MaxSymbols = 40

// maxSymbolsFileBytes keeps a misplaced file (for example a log) from being
// read whole before it is rejected.
const maxSymbolsFileBytes = 64 << 10

// ErrSymbols marks every rejection of the symbol list file.
var ErrSymbols = errors.New("quote: symbol list rejected")

// LoadSymbols reads the symbol list file: one KRX short code per line, six
// characters of 0-9 or A-Z. Blank lines are ignored and '#' starts a comment
// that runs to the end of the line. An empty list, a duplicate, a malformed
// line, or more than MaxSymbols codes rejects the whole file.
func LoadSymbols(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read file", ErrSymbols)
	}
	if !info.Mode().IsRegular() || info.Size() > maxSymbolsFileBytes {
		return nil, fmt.Errorf("%w: not a regular file of at most %d bytes", ErrSymbols, maxSymbolsFileBytes)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read file", ErrSymbols)
	}
	return ParseSymbols(contents)
}

// ParseSymbols applies the LoadSymbols format to contents.
func ParseSymbols(contents []byte) ([]string, error) {
	if len(contents) > maxSymbolsFileBytes {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrSymbols, maxSymbolsFileBytes)
	}
	var symbols []string
	seen := map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	scanner.Buffer(make([]byte, 0, 1024), maxSymbolsFileBytes)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if index := strings.IndexByte(text, '#'); index >= 0 {
			text = text[:index]
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		if !ValidSymbol(text) {
			return nil, fmt.Errorf("%w: line %d is not a six-character KRX code", ErrSymbols, line)
		}
		if first, ok := seen[text]; ok {
			return nil, fmt.Errorf("%w: line %d repeats line %d", ErrSymbols, line, first)
		}
		seen[text] = line
		symbols = append(symbols, text)
		if len(symbols) > MaxSymbols {
			return nil, fmt.Errorf("%w: more than %d symbols", ErrSymbols, MaxSymbols)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: unreadable line", ErrSymbols)
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("%w: no symbols", ErrSymbols)
	}
	return symbols, nil
}

// ValidSymbol reports whether s is a six-character KRX short code made of
// digits and upper-case letters (for example 005930 or 0001A0).
func ValidSymbol(s string) bool {
	if len(s) != 6 {
		return false
	}
	for index := 0; index < len(s); index++ {
		c := s[index]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}
