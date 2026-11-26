package nhplugws

import (
	"bytes"
	"encoding/json"
	"time"
	"unicode/utf8"
)

// Registration is one NHPLUG realtime channel/key pair.
type Registration struct {
	TRCD  string `toml:"tr_cd" json:"tr_cd"`
	TRKey string `toml:"tr_key" json:"tr_key"`
}

// Record preserves a documented inbound frame exactly as received.
type Record struct {
	Kind       string          `json:"kind"`
	ReceivedAt time.Time       `json:"received_at"`
	TRCD       string          `json:"tr_cd,omitempty"`
	TRKey      json.RawMessage `json:"tr_key,omitempty"`
	Raw        json.RawMessage `json:"raw"`
}

type inboundFrame struct {
	Header map[string]json.RawMessage `json:"header"`
	Body   json.RawMessage            `json:"body"`
}

func parseFrame(raw []byte) (Record, bool) {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return Record{}, false
	}
	var frame inboundFrame
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&frame) != nil || frame.Header == nil || len(frame.Body) == 0 || !json.Valid(frame.Body) {
		return Record{}, false
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(frame.Body, &body) != nil || body == nil {
		return Record{}, false
	}
	var code string
	if value := frame.Header["tr_cd"]; len(value) > 0 && json.Unmarshal(value, &code) != nil {
		return Record{}, false
	}
	if code == "" {
		// ACK frames carry tr_cd in header and are still required to name it.
		return Record{}, false
	}
	var trKey json.RawMessage
	if rawKey := frame.Header["tr_key"]; len(rawKey) > 0 {
		trKey = rawKey
	} else if rawKey := body["tr_key"]; len(rawKey) > 0 {
		trKey = rawKey
	}
	return Record{Kind: "frame", TRCD: code, TRKey: trKey, Raw: append(json.RawMessage(nil), raw...)}, true
}
