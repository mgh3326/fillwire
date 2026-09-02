// Package kislive declares live KIS trading identifiers for audit review only.
// It has no host, HTTP client, request builder, token, or transport code.
package kislive

// TRTable is deliberately inert in Phase 1. Phase 2 requires a separate
// approval before any request-construction code may reference these values.
var TRTable = map[string]map[string]string{
	"krx": {"buy": "TTTC0012U", "sell": "TTTC0011U", "cancel": "TTTC0013U"},
	"us":  {"buy": "TTTT1002U", "sell": "TTTT1001U", "cancel": "TTTT1004U"},
}
