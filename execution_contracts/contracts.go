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
	// AccountScopeAlpacaPaperCrypto routes a command to the Alpaca paper
	// crypto backend. It never represents Alpaca's live trading authority.
	AccountScopeAlpacaPaperCrypto = "alpaca_paper_crypto"
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
// known; no upstream response payload is included.
type ExecutionReceiptV1 struct {
	SchemaVersion string               `json:"schema_version"`
	CommandID     string               `json:"command_id"`
	Disposition   ExecutionDisposition `json:"disposition"`
	BrokerOrderID string               `json:"broker_order_id,omitempty"`
	ErrorCode     string               `json:"error_code,omitempty"`
	RecordedAt    string               `json:"recorded_at"`
}

// TokenLeaseView exposes validity metadata only. It intentionally never carries
// a token value.
type TokenLeaseView struct {
	SchemaVersion string  `json:"schema_version"`
	ExpiresAt     float64 `json:"expires_at"`
	Valid         bool    `json:"valid"`
}
