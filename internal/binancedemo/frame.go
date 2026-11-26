package binancedemo

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
)

var (
	symbolPattern        = regexp.MustCompile(`^[A-Z0-9]{5,20}$`)
	decimalPattern       = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)
	signedDecimalPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
)

func validSymbol(s string) bool { return symbolPattern.MatchString(s) }

func decodeObject(raw []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, errors.New("not a JSON object")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing JSON value")
	}
	return obj, nil
}

func validateMarketFrame(raw []byte, symbols []string) (string, string, error) {
	obj, err := decodeObject(raw)
	if err != nil {
		return "", "", err
	}
	streamName, ok := obj["stream"].(string)
	if !ok {
		return "", "", errors.New("stream is missing")
	}
	parts := strings.Split(streamName, "@")
	if len(parts) != 2 || parts[0] == "" {
		return "", "", errors.New("invalid stream name")
	}
	symbol := strings.ToUpper(parts[0])
	if !validSymbol(symbol) || !contains(symbols, symbol) {
		return "", "", errors.New("unknown stream symbol")
	}
	data, ok := obj["data"].(map[string]any)
	if !ok {
		return "", "", errors.New("data is missing")
	}
	switch parts[1] {
	case "depth20":
		if !validInt(data["lastUpdateId"]) || !validLevels(data["bids"]) || !validLevels(data["asks"]) {
			return "", "", errors.New("invalid depth payload")
		}
	case "bookTicker":
		if !matchesSymbol(data, symbol) || !validInt(data["u"]) || !validDecimal(data["b"]) ||
			!validDecimal(data["B"]) || !validDecimal(data["a"]) || !validDecimal(data["A"]) {
			return "", "", errors.New("invalid book ticker payload")
		}
	case "aggTrade":
		if !matchesSymbol(data, symbol) || !validInt(data["a"]) || !validDecimal(data["p"]) ||
			!validDecimal(data["q"]) || !validInt(data["T"]) {
			return "", "", errors.New("invalid aggregate trade payload")
		}
		if _, ok := data["m"].(bool); !ok {
			return "", "", errors.New("invalid aggregate trade maker flag")
		}
	default:
		return "", "", errors.New("unsupported market channel")
	}
	return streamName, symbol, nil
}

func validateUserFrame(raw []byte) (string, error) {
	obj, err := decodeObject(raw)
	if err != nil {
		return "", err
	}
	event, ok := obj["event"].(map[string]any)
	if !ok {
		return "", errors.New("event is missing")
	}
	typeName, ok := event["e"].(string)
	if !ok || typeName == "" {
		return "", errors.New("event type is missing")
	}
	if typeName == "serverShutdown" {
		return typeName, nil
	}
	switch typeName {
	case "executionReport":
		if !validInt(event["E"]) || !nonEmptyStrings(event, "s", "c", "S", "o", "x", "X") ||
			!validInt(event["i"]) || !validDecimal(event["q"]) || !validDecimal(event["p"]) ||
			!validDecimal(event["l"]) || !validDecimal(event["z"]) || !validDecimal(event["L"]) ||
			!validInt(event["t"]) || !validInt(event["T"]) {
			return "", errors.New("invalid execution report")
		}
	case "outboundAccountPosition":
		if !validInt(event["E"]) {
			return "", errors.New("invalid account event time")
		}
		balances, ok := event["B"].([]any)
		if !ok {
			return "", errors.New("invalid account balances")
		}
		for _, item := range balances {
			balance, ok := item.(map[string]any)
			if !ok || !nonEmptyStrings(balance, "a") || !validDecimal(balance["f"]) || !validDecimal(balance["l"]) {
				return "", errors.New("invalid account balance")
			}
		}
	case "balanceUpdate":
		if !validInt(event["E"]) || !nonEmptyStrings(event, "a") || !validSignedDecimal(event["d"]) {
			return "", errors.New("invalid balance update")
		}
	default:
		// Preserve documented but currently unclassified user events as raw
		// observations; they never enter an order or ledger path.
		if _, ok := event["E"]; ok && !validInt(event["E"]) {
			return "", errors.New("invalid user event time")
		}
	}
	return typeName, nil
}

func contains(symbols []string, symbol string) bool {
	for _, candidate := range symbols {
		if candidate == symbol {
			return true
		}
	}
	return false
}

func matchesSymbol(data map[string]any, symbol string) bool {
	got, ok := data["s"].(string)
	return ok && strings.ToUpper(got) == symbol
}

func nonEmptyStrings(obj map[string]any, keys ...string) bool {
	for _, key := range keys {
		if s, ok := obj[key].(string); !ok || s == "" {
			return false
		}
	}
	return true
}

func validInt(v any) bool {
	n, ok := v.(json.Number)
	if !ok {
		return false
	}
	_, err := n.Int64()
	return err == nil
}

func validDecimal(v any) bool {
	switch n := v.(type) {
	case string:
		return decimalPattern.MatchString(n)
	case json.Number:
		return decimalPattern.MatchString(n.String())
	default:
		return false
	}
}

func validSignedDecimal(v any) bool {
	switch n := v.(type) {
	case string:
		return signedDecimalPattern.MatchString(n)
	case json.Number:
		return signedDecimalPattern.MatchString(n.String())
	default:
		return false
	}
}

func validLevels(v any) bool {
	levels, ok := v.([]any)
	if !ok {
		return false
	}
	for _, item := range levels {
		level, ok := item.([]any)
		if !ok || len(level) < 2 || !validDecimal(level[0]) || !validDecimal(level[1]) {
			return false
		}
	}
	return true
}
