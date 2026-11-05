// Package exitcode is the one process exit-code table every fillwire binary
// uses, so a supervisor or an operator script reads the same number the same
// way whichever command produced it.
package exitcode

const (
	// OK is a clean exit.
	OK = 0
	// Failure is a runtime failure that a retry or restart may clear:
	// network, Redis, broker 5xx, lock timeouts, unreadable input files.
	Failure = 1
	// Usage is a bad command line: unknown flags, missing or conflicting
	// arguments. Nothing was read or sent.
	Usage = 2
	// SessionOccupied is reserved for KIS OPSP8996 (another holder owns the
	// websocket session). It is never retried in-process.
	SessionOccupied = 42
	// Config is a configuration error (EX_CONFIG): invalid settings, missing
	// credentials, or a policy refusal that needs operator action. The
	// systemd unit does not restart on it.
	Config = 78
)
