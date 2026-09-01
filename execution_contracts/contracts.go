// Package executioncontracts contains deliberately small transport contracts.
//
// These are schema names, not a domain ledger or a broker gateway.
package executioncontracts

// ExecutionCommandV1 is the minimum identity of a requested execution action.
// It has no broker credentials, token, or transport-specific field.
type ExecutionCommandV1 struct {
	SchemaVersion string `json:"schema_version"`
	CommandID     string `json:"command_id"`
	Kind          string `json:"kind"`
}

// ExecutionReceiptV1 is the minimum acknowledgement of an execution command.
type ExecutionReceiptV1 struct {
	SchemaVersion string `json:"schema_version"`
	CommandID     string `json:"command_id"`
	Status        string `json:"status"`
}

// TokenLeaseView exposes validity metadata only. It intentionally never carries
// a token value.
type TokenLeaseView struct {
	SchemaVersion string  `json:"schema_version"`
	ExpiresAt     float64 `json:"expires_at"`
	Valid         bool    `json:"valid"`
}
