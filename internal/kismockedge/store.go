package kismockedge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	_ "modernc.org/sqlite"
)

// Store holds durable receipts, the pending send marker, and the minimum
// command facts required to reconcile a post-send UNKNOWN. It never stores
// credentials, tokens, broker payloads, or account numbers.
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
			phase TEXT NOT NULL CHECK (phase IN ('pending', 'final')),
			account_scope TEXT NOT NULL DEFAULT 'kis_mock'
		)
	`); err != nil {
		_ = database.Close()
		return nil, err
	}
	// account_scope was added after the initial edge release. The edge has
	// always been mock-only, so the default safely classifies legacy rows.
	if err := ensureColumn(database, "commands", "account_scope", "TEXT NOT NULL DEFAULT 'kis_mock'"); err != nil {
		_ = database.Close()
		return nil, err
	}
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS command_contexts (
			command_id TEXT PRIMARY KEY REFERENCES commands(command_id),
			side TEXT NOT NULL,
			stock_code TEXT NOT NULL,
			quantity TEXT NOT NULL,
			price TEXT NOT NULL,
			sent_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS command_resolutions (
			command_id TEXT PRIMARY KEY REFERENCES commands(command_id),
			disposition TEXT NOT NULL CHECK (disposition IN ('NOT_CREATED', 'ACCEPTED')),
			broker_order_id TEXT,
			error_code TEXT,
			resolved_at TEXT NOT NULL
		)
	`); err != nil {
		_ = database.Close()
		return nil, err
	}
	return &Store{db: database}, nil
}

func ensureColumn(database *sql.DB, table, column, definition string) error {
	rows, err := database.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = database.Exec("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition)
	return err
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
		SELECT c.schema_version, c.command_id, COALESCE(r.disposition, c.disposition),
			CASE WHEN r.command_id IS NULL THEN c.broker_order_id ELSE r.broker_order_id END,
			CASE WHEN r.command_id IS NULL THEN c.error_code ELSE r.error_code END, c.recorded_at
		FROM commands c LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		WHERE c.command_id = ?
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
func (store *Store) ReservePending(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1, command executioncontracts.ExecutionCommandV1) (executioncontracts.ExecutionReceiptV1, bool, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("store unavailable")
	}
	if !validReceipt(receipt) || receipt.Disposition != executioncontracts.DispositionUnknown ||
		command.CommandID != receipt.CommandID || command.AccountScope != executioncontracts.AccountScopeKISMock {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("invalid pending receipt")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO commands (command_id, schema_version, disposition, broker_order_id, error_code, recorded_at, phase, account_scope)
		VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, 'pending', ?)
		ON CONFLICT(command_id) DO NOTHING
	`, receipt.CommandID, receipt.SchemaVersion, receipt.Disposition, receipt.BrokerOrderID, receipt.ErrorCode, receipt.RecordedAt, command.AccountScope)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
	}
	if inserted == 1 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO command_contexts (command_id, side, stock_code, quantity, price, sent_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, command.CommandID, command.Side, command.StockCode, command.Quantity, command.Price, receipt.RecordedAt); err != nil {
			return executioncontracts.ExecutionReceiptV1{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return executioncontracts.ExecutionReceiptV1{}, false, err
		}
		return receipt, true, nil
	}
	if err := tx.Commit(); err != nil {
		return executioncontracts.ExecutionReceiptV1{}, false, err
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

// PendingResolution is an UNKNOWN mock receipt accompanied by its immutable
// send facts. ContextPresent is false for rows written before reconciliation
// support; those rows can only be resolved absent by an empty day result.
type PendingResolution struct {
	Receipt        executioncontracts.ExecutionReceiptV1
	AccountScope   string
	Side           string
	StockCode      string
	Quantity       string
	Price          string
	SentAt         time.Time
	ContextPresent bool
}

// PendingKISMockResolutions returns only unresolved UNKNOWN records in this
// mock-only store. It never changes a receipt.
func (store *Store) PendingKISMockResolutions(ctx context.Context) ([]PendingResolution, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("store unavailable")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT c.schema_version, c.command_id, c.disposition, c.broker_order_id, c.error_code, c.recorded_at,
			c.account_scope, x.side, x.stock_code, x.quantity, x.price, x.sent_at
		FROM commands c
		LEFT JOIN command_contexts x ON x.command_id = c.command_id
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		WHERE c.disposition = 'UNKNOWN' AND c.account_scope = ? AND r.command_id IS NULL
		ORDER BY c.id
	`, executioncontracts.AccountScopeKISMock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PendingResolution
	for rows.Next() {
		var item PendingResolution
		var brokerOrderID, errorCode, side, stockCode, quantity, price, sentAt sql.NullString
		if err := rows.Scan(&item.Receipt.SchemaVersion, &item.Receipt.CommandID, &item.Receipt.Disposition,
			&brokerOrderID, &errorCode, &item.Receipt.RecordedAt, &item.AccountScope,
			&side, &stockCode, &quantity, &price, &sentAt); err != nil {
			return nil, err
		}
		if brokerOrderID.Valid {
			item.Receipt.BrokerOrderID = brokerOrderID.String
		}
		if errorCode.Valid {
			item.Receipt.ErrorCode = errorCode.String
		}
		if !validReceipt(item.Receipt) {
			return nil, errors.New("invalid stored receipt")
		}
		item.ContextPresent = side.Valid && stockCode.Valid && quantity.Valid && price.Valid && sentAt.Valid
		if item.ContextPresent {
			item.Side, item.StockCode = side.String, stockCode.String
			item.Quantity, item.Price = quantity.String, price.String
			var parseErr error
			item.SentAt, parseErr = time.Parse(time.RFC3339Nano, sentAt.String)
			if parseErr != nil {
				return nil, errors.New("invalid stored command context")
			}
		} else {
			var parseErr error
			item.SentAt, parseErr = time.Parse(time.RFC3339Nano, item.Receipt.RecordedAt)
			if parseErr != nil {
				return nil, errors.New("invalid stored receipt")
			}
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

// ResolveUnknown appends a conclusive reconciliation record and returns the
// resulting receipt. It never overwrites the original receipt row.
func (store *Store) ResolveUnknown(ctx context.Context, commandID string, disposition executioncontracts.ExecutionDisposition, brokerOrderID, errorCode string, resolvedAt time.Time) (executioncontracts.ExecutionReceiptV1, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("store unavailable")
	}
	if !validCommandID(commandID) || (disposition != executioncontracts.DispositionAccepted && disposition != executioncontracts.DispositionNotCreated) ||
		(disposition == executioncontracts.DispositionAccepted && brokerOrderID == "") ||
		(disposition == executioncontracts.DispositionNotCreated && errorCode == "") {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("invalid resolution")
	}
	result, err := store.db.ExecContext(ctx, `
		INSERT INTO command_resolutions (command_id, disposition, broker_order_id, error_code, resolved_at)
		SELECT command_id, ?, NULLIF(?, ''), NULLIF(?, ''), ? FROM commands
		WHERE command_id = ? AND disposition = 'UNKNOWN'
		ON CONFLICT(command_id) DO NOTHING
	`, disposition, brokerOrderID, errorCode, resolvedAt.UTC().Format(time.RFC3339Nano), commandID)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	_, err = result.RowsAffected()
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	receipt, found, err := store.Find(ctx, commandID)
	if err != nil {
		return executioncontracts.ExecutionReceiptV1{}, err
	}
	if !found {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("receipt missing")
	}
	return receipt, nil
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
