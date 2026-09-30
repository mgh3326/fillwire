// Package executioncontracts contains deliberately small transport contracts.
//
// These are schema names, not a domain ledger or a broker gateway.
package executioncontracts

import (
	"encoding/json"
	"fmt"
)

const (
	// ExecutionCommandV1SchemaVersion identifies the first command wire shape.
	ExecutionCommandV1SchemaVersion = "execution-command/v1"
	// ExecutionReceiptV1SchemaVersion identifies the first receipt wire shape.
	ExecutionReceiptV1SchemaVersion = "execution-receipt/v1"
	// AccountScopeKISMock routes a command to the KIS VTS backend.
	AccountScopeKISMock = "kis_mock"
	// AccountScopeKISMockUS routes a command to the KIS VTS US-equity
	// backend. It is intentionally distinct from the domestic KIS mock scope.
	AccountScopeKISMockUS = "kis_mock_us"
	// AccountScopeAlpacaPaperCrypto routes a command to the Alpaca paper
	// crypto backend. It never represents Alpaca's live trading authority.
	AccountScopeAlpacaPaperCrypto = "alpaca_paper_crypto"
	// AccountScopeKISLive is accepted only by the edge's shadow witness. It
	// never authorizes an edge-to-broker request.
	AccountScopeKISLive = "kis_live"
)

// ExecutionCommandV1 is the narrow, mock-only request accepted by
// kis-mock-edge. Quantity and Price intentionally remain strings: the edge
// validates them but never normalizes or re-prices them.
type ExecutionCommandV1 struct {
	SchemaVersion string `json:"schema_version"`
	CommandID     string `json:"command_id"`
	AccountScope  string `json:"account_scope"`
	Side          string `json:"side"`
	StockCode     string `json:"stock_code"`
	Quantity      string `json:"quantity"`
	Price         string `json:"price"`
	OrderType     string `json:"order_type"`
	IssuedAt      string `json:"issued_at"`
}

// ExecutionDisposition is deliberately closed. In particular, callers must
// not infer NOT_CREATED after the broker send boundary has been crossed.
type ExecutionDisposition string

const (
	DispositionNotCreated ExecutionDisposition = "NOT_CREATED"
	DispositionAccepted   ExecutionDisposition = "ACCEPTED"
	DispositionUnknown    ExecutionDisposition = "UNKNOWN"
)

// Valid reports whether disposition belongs to the closed receipt vocabulary.
func (disposition ExecutionDisposition) Valid() bool {
	switch disposition {
	case DispositionNotCreated, DispositionAccepted, DispositionUnknown:
		return true
	default:
		return false
	}
}

// MarshalJSON refuses values outside the receipt vocabulary at the transport
// boundary as well as in service validation.
func (disposition ExecutionDisposition) MarshalJSON() ([]byte, error) {
	if !disposition.Valid() {
		return nil, fmt.Errorf("invalid execution disposition")
	}
	return json.Marshal(string(disposition))
}

// UnmarshalJSON accepts only the three declared receipt dispositions.
func (disposition *ExecutionDisposition) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	parsed := ExecutionDisposition(value)
	if !parsed.Valid() {
		return fmt.Errorf("invalid execution disposition")
	}
	*disposition = parsed
	return nil
}

// ExecutionReceiptV1 is the durable acknowledgement of an execution command.
// BrokerOrderID and ErrorCode are absent unless the corresponding fact is
// known; no upstream response payload is included. Rejection is the single
// exception: it carries only the masked failure fields declared by
// BrokerRejectionV1 and exists on the response alone.
type ExecutionReceiptV1 struct {
	SchemaVersion string               `json:"schema_version"`
	CommandID     string               `json:"command_id"`
	Disposition   ExecutionDisposition `json:"disposition"`
	BrokerOrderID string               `json:"broker_order_id,omitempty"`
	ErrorCode     string               `json:"error_code,omitempty"`
	RecordedAt    string               `json:"recorded_at"`
	Rejection     *BrokerRejectionV1   `json:"rejection,omitempty"`
}

// BrokerRejectionV1 carries a broker response's own failure fields so a
// rejected place or cancel leaves evidence. Every text value is masked at
// capture and capped in length: account numbers, access tokens, and
// application keys can never appear here. It is response-only and is never
// written to durable storage.
type BrokerRejectionV1 struct {
	RtCd       string `json:"rt_cd,omitempty"`
	MsgCd      string `json:"msg_cd,omitempty"`
	Msg1       string `json:"msg1,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

// PriceBandV1SchemaVersion identifies the read-only daily price-band wire
// shape served by the edge's price-band endpoint.
const PriceBandV1SchemaVersion = "price-band/v1"

// PriceBandV1 is the edge's read-only answer for one domestic mock symbol's
// daily price band. Every price is a decimal digit string; the shape carries
// no account, order, token, or credential material.
type PriceBandV1 struct {
	SchemaVersion string `json:"schema_version"`
	StockCode     string `json:"stock_code"`
	LastPrice     string `json:"last_price,omitempty"`
	UpperLimit    string `json:"upper_limit,omitempty"`
	LowerLimit    string `json:"lower_limit,omitempty"`
	BasePrice     string `json:"base_price,omitempty"`
}

// CommandCheckV1SchemaVersion identifies the bounded single-command evidence
// answer returned by the edge's per-command resolve endpoint.
const CommandCheckV1SchemaVersion = "command-check/v1"

// CommandCheckV1 is the edge's answer for one command's post-send evidence
// check. It carries the command's effective disposition plus count-only
// evidence fields; it never carries order, account, or token values.
type CommandCheckV1 struct {
	SchemaVersion string               `json:"schema_version"`
	CommandID     string               `json:"command_id"`
	Disposition   ExecutionDisposition `json:"disposition"`
	BrokerOrderID string               `json:"broker_order_id,omitempty"`
	ErrorCode     string               `json:"error_code,omitempty"`
	// EvidenceRead is "completed" when the broker day read ran, "not_needed"
	// when the stored disposition was already conclusive, and "unavailable"
	// when no evidence source can address this command.
	EvidenceRead string `json:"evidence_read"`
	// OrdersSeen is the number of orders the completed day read returned. It
	// stays 0 for the Alpaca by-client-order-id read and for any state where
	// no read ran.
	OrdersSeen int `json:"orders_seen"`
	// Matched is the number of orders matching this command's stored facts.
	Matched int `json:"matched"`
}

// TokenLeaseView exposes validity metadata only. It intentionally never carries
// a token value.
type TokenLeaseView struct {
	SchemaVersion string  `json:"schema_version"`
	ExpiresAt     float64 `json:"expires_at"`
	Valid         bool    `json:"valid"`
}
