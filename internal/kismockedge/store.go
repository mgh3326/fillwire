package kismockedge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	_ "modernc.org/sqlite"
)

// Store holds only durable receipts and the pending send marker. It does not
// store credentials, tokens, broker payloads, account numbers, or order data.
type Store struct {
	db *sql.DB
}

// OpenStore opens and initializes a local SQLite receipt database.
func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite path missing")
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// A single connection makes the in-memory test form deterministic while
	// SQLite's UNIQUE command_id constraint remains the cross-process guard.
	database.SetMaxOpenConns(1)
	if _, err := database.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		_ = database.Close()
		return nil, err
	}
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS commands (
			id INTEGER PRIMARY KEY,
			command_id TEXT NOT NULL UNIQUE,
			schema_version TEXT NOT NULL,
			disposition TEXT NOT NULL CHECK (disposition IN ('NOT_CREATED', 'ACCEPTED', 'UNKNOWN')),
			broker_order_id TEXT,
			error_code TEXT,
			recorded_at TEXT NOT NULL,
			phase TEXT NOT NULL CHECK (phase IN ('pending', 'final'))
		)
	`); err != nil {
		_ = database.Close()
		return nil, err
	}
	return &Store{db: database}, nil
}

// Close releases the local database handle.
func (store *Store) Close() error {
	if store == nil || store.db == nil {
		return nil
	}
	return store.db.Close()
}

// Find returns the already-durable receipt, including an UNKNOWN pending
// marker left by a process that died immediately after committing it.
func (store *Store) Find(ctx context.Context, commandID string) (executioncontracts.ExecutionReceiptV1, bool, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("store unavailable")
	}
	row := store.db.QueryRowContext(ctx, `
		SELECT schema_version, command_id, disposition, broker_order_id, error_code, recorded_at
		FROM commands WHERE command_id = ?
	`, commandID)
	receipt, err := scanReceipt(row)
	if errors.Is(err, sql.ErrNoRows) {
		return executioncontracts.ExecutionReceiptV1{}, false, nil
	}
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	return receipt, true, nil
}

// StoreFinal inserts a receipt that is conclusively NOT_CREATED before any
// broker send. A conflict returns the original durable receipt unchanged.
func (store *Store) StoreFinal(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1) (executioncontracts.ExecutionReceiptV1, bool, error) {
	return store.insert(ctx, receipt, "final")
}

// ReservePending is the pre-send durability boundary. Its successful return
// means the UNKNOWN receipt has been committed before Broker.Send may begin.
func (store *Store) ReservePending(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1) (executioncontracts.ExecutionReceiptV1, bool, error) {
	return store.insert(ctx, receipt, "pending")
}

func (store *Store) insert(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1, phase string) (executioncontracts.ExecutionReceiptV1, bool, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("store unavailable")
	}
	if !validReceipt(receipt) || (phase != "pending" && phase != "final") {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("invalid receipt")
	}
	result, err := store.db.ExecContext(ctx, `
		INSERT INTO commands (command_id, schema_version, disposition, broker_order_id, error_code, recorded_at, phase)
		VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)
		ON CONFLICT(command_id) DO NOTHING
	`, receipt.CommandID, receipt.SchemaVersion, receipt.Disposition, receipt.BrokerOrderID, receipt.ErrorCode, receipt.RecordedAt, phase)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	if inserted == 1 {
		return receipt, true, nil
	}
	existing, found, err := store.Find(ctx, receipt.CommandID)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("receipt disappeared after command_id conflict")
		}
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	return existing, false, nil
}

// Finalize replaces only the process owner's pending marker. It cannot create
// a new command after the send boundary.
func (store *Store) Finalize(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1) (executioncontracts.ExecutionReceiptV1, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("store unavailable")
	}
	if !validReceipt(receipt) {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("invalid receipt")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE commands
		SET schema_version = ?, disposition = ?, broker_order_id = NULLIF(?, ''),
			error_code = NULLIF(?, ''), recorded_at = ?, phase = 'final'
		WHERE command_id = ? AND phase = 'pending'
	`, receipt.SchemaVersion, receipt.Disposition, receipt.BrokerOrderID, receipt.ErrorCode, receipt.RecordedAt, receipt.CommandID)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	if updated == 1 {
		return receipt, nil
	}
	existing, found, err := store.Find(ctx, receipt.CommandID)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	if !found {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("pending receipt missing")
	}
	return existing, nil
}

type receiptRow interface {
	Scan(dest ...any) error
}

func scanReceipt(row receiptRow) (executioncontracts.ExecutionReceiptV1, error) {
	var receipt executioncontracts.ExecutionReceiptV1
	var brokerOrderID sql.NullString
	var errorCode sql.NullString
	if err := row.Scan(
		&receipt.SchemaVersion,
		&receipt.CommandID,
		&receipt.Disposition,
		&brokerOrderID,
		&errorCode,
		&receipt.RecordedAt,
	); err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	if !validReceipt(receipt) {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("invalid stored receipt")
	}
	if brokerOrderID.Valid {
		receipt.BrokerOrderID = brokerOrderID.String
	}
	if errorCode.Valid {
		receipt.ErrorCode = errorCode.String
	}
	return receipt, nil
}

func validReceipt(receipt executioncontracts.ExecutionReceiptV1) bool {
	return receipt.SchemaVersion == executioncontracts.ExecutionReceiptV1SchemaVersion &&
		validCommandID(receipt.CommandID) &&
		receipt.Disposition.Valid() &&
		receipt.RecordedAt != ""
}
