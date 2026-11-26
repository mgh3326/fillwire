package tossws

import (
	"bytes"
	"encoding/json"
	"regexp"
	"time"
	"unicode/utf8"
)

const channel = "personal:order"

var (
	accountPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
	decimalPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
)

func ValidAccountSeq(seq string) bool { return accountPattern.MatchString(seq) }

type Record struct {
	Kind       string          `json:"kind"`
	ReceivedAt time.Time       `json:"received_at"`
	SessionID  string          `json:"session_id"`
	Topic      string          `json:"topic"`
	Data       json.RawMessage `json:"data,omitempty"`
	Raw        json.RawMessage `json:"raw,omitempty"`
}

type object = map[string]any

func parseFrame(raw []byte) object {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var frame object
	if decoder.Decode(&frame) != nil {
		return nil
	}
	return frame
}

// The AsyncAPI explicitly permits unknown codes for several string fields.
// Only fixed protocol discriminators and the side enum are closed.
func orderData(frame object, topic, account string) bool {
	data, _ := frame["data"].(object)
	order, _ := data["order"].(object)
	execution, _ := order["execution"].(object)
	if frame["topic"] != topic || data["accountSeq"] != account ||
		!stringsPresent(data, "event") ||
		!stringsPresent(order, "orderId", "symbol", "orderType", "timeInForce", "status", "currency") ||
		(order["side"] != "BUY" && order["side"] != "SELL") {
		return false
	}
	return decimal(order["quantity"]) && decimal(execution["filledQuantity"]) &&
		nullableDecimals(order, false, "price", "orderAmount") &&
		nullableDecimals(execution, true, "averageFilledPrice", "filledAmount", "commission", "tax") &&
		dateField(order, "orderedAt", time.RFC3339, true, false) &&
		dateField(order, "canceledAt", time.RFC3339, false, true) &&
		dateField(execution, "settlementDate", time.DateOnly, true, true)
}

func stringsPresent(obj object, keys ...string) bool {
	for _, key := range keys {
		if s, ok := obj[key].(string); !ok || s == "" {
			return false
		}
	}
	return true
}

func decimal(value any) bool {
	s, ok := value.(string)
	return ok && len(s) <= 30 && decimalPattern.MatchString(s)
}

func nullableDecimals(obj object, required bool, keys ...string) bool {
	for _, key := range keys {
		value, present := obj[key]
		if (!present && required) || (value != nil && !decimal(value)) {
			return false
		}
	}
	return true
}

func dateField(obj object, key, layout string, required, nullable bool) bool {
	value, present := obj[key]
	if !present {
		return !required
	}
	if value == nil {
		return nullable
	}
	s, ok := value.(string)
	_, err := time.Parse(layout, s)
	return ok && err == nil
}
