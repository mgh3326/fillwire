// Package quote streams Toss Securities realtime trade and order-book quotes
// into a Redis Stream.
//
// It is a separate lane from the execution-fill pipeline. It owns its own
// websocket, Redis client, and goroutines, reads the shared Toss access token
// without ever issuing or refreshing it, and never places orders, applies
// policy, or writes to the execution ledger.
package quote

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
)

// MaxSymbols bounds the configured symbol list. Each symbol takes two of the
// 100 subscriptions Toss allows per connection (trade and orderbook), so 40
// symbols use 80. A longer list is refused rather than truncated.
const MaxSymbols = 40

// maxSymbolsFileBytes keeps a misplaced file (for example a log) from being
// read whole before it is rejected.
const maxSymbolsFileBytes = 64 << 10

// Markets.
const (
	MarketKR = "kr"
	MarketUS = "us"
)

// ErrSymbols marks every rejection of the symbol list file.
var ErrSymbols = errors.New("quote: symbol list rejected")

// Symbol is one configured instrument.
type Symbol struct {
	Market string // MarketKR or MarketUS
	Code   string // KR six-character code or US ticker, as the Toss master spells it
}

// LoadSymbols reads the symbol list file: one "<market> <code>" pair per line,
// where market is kr or us. Blank lines are ignored and '#' starts a comment
// that runs to the end of the line. An empty list, a duplicate, a malformed
// line, or more than MaxSymbols entries rejects the whole file.
func LoadSymbols(path string) ([]Symbol, error) {
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
func ParseSymbols(contents []byte) ([]Symbol, error) {
	if len(contents) > maxSymbolsFileBytes {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrSymbols, maxSymbolsFileBytes)
	}
	var symbols []Symbol
	seen := map[Symbol]int{}
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	scanner.Buffer(make([]byte, 0, 1024), maxSymbolsFileBytes)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if index := strings.IndexByte(text, '#'); index >= 0 {
			text = text[:index]
		}
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 {
			return nil, fmt.Errorf("%w: line %d is not \"<market> <code>\"", ErrSymbols, line)
		}
		symbol := Symbol{Market: fields[0], Code: fields[1]}
		if !ValidSymbol(symbol) {
			return nil, fmt.Errorf("%w: line %d has an invalid market or code", ErrSymbols, line)
		}
		if first, ok := seen[symbol]; ok {
			return nil, fmt.Errorf("%w: line %d repeats line %d", ErrSymbols, line, first)
		}
		seen[symbol] = line
		symbols = append(symbols, symbol)
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

// ValidSymbol reports whether s names a supported market and a well-formed
// code: KR takes six characters of 0-9 or A-Z (for example 005930 or 0001A0);
// US takes an upper-case ticker of 1 to 10 characters from A-Z, 0-9, '.' and
// '-' that starts with a letter (for example AAPL or BRK.B). Toss rejects a
// code missing from its master with stock-not-found at subscribe time.
func ValidSymbol(s Symbol) bool {
	switch s.Market {
	case MarketKR:
		if len(s.Code) != 6 {
			return false
		}
		for index := 0; index < len(s.Code); index++ {
			if !upperOrDigit(s.Code[index]) {
				return false
			}
		}
		return true
	case MarketUS:
		if len(s.Code) < 1 || len(s.Code) > 10 || s.Code[0] < 'A' || s.Code[0] > 'Z' {
			return false
		}
		for index := 0; index < len(s.Code); index++ {
			c := s.Code[index]
			if !upperOrDigit(c) && c != '.' && c != '-' {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func upperOrDigit(c byte) bool { return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') }
