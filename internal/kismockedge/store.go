package kismockedge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
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
	// Live intents are witnesses, not command receipts. A separate table keeps
	// them structurally unable to enter the mock send/cancel lifecycle.
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS kis_live_witnesses (
			command_id TEXT PRIMARY KEY,
			witness_id TEXT NOT NULL UNIQUE,
			schema_version TEXT NOT NULL,
			account_scope TEXT NOT NULL CHECK (account_scope = 'kis_live'),
			side TEXT NOT NULL,
			stock_code TEXT NOT NULL,
			quantity TEXT NOT NULL,
			price TEXT NOT NULL,
			order_type TEXT NOT NULL,
			issued_at TEXT NOT NULL,
			phase TEXT NOT NULL CHECK (phase = 'shadow'),
			received_at TEXT NOT NULL,
			echo_at TEXT,
			broker_order_id TEXT,
			broker_rt_cd TEXT,
			broker_msg_cd TEXT,
			broker_msg1 TEXT,
			broker_received_at TEXT
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
	if err := ensureColumn(database, "commands", "krx_fwdg_ord_orgno", "TEXT"); err != nil {
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
			sent_at TEXT NOT NULL,
			client_order_id TEXT
		);
		CREATE TABLE IF NOT EXISTS command_resolutions (
			command_id TEXT PRIMARY KEY REFERENCES commands(command_id),
			disposition TEXT NOT NULL CHECK (disposition IN ('NOT_CREATED', 'ACCEPTED')),
			broker_order_id TEXT,
			error_code TEXT,
			resolved_at TEXT NOT NULL
		)
		;
		CREATE TABLE IF NOT EXISTS cancel_attempts (
			command_id TEXT NOT NULL REFERENCES commands(command_id),
			attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
			state TEXT NOT NULL CHECK (state IN ('CANCELLED', 'NOT_FOUND', 'UNKNOWN', 'REJECTED_THROTTLED')),
			error_code TEXT,
			recorded_at TEXT NOT NULL,
			PRIMARY KEY (command_id, attempt_no)
		)
	`); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := migrateCancelAttempts(database); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := ensureColumn(database, "command_contexts", "client_order_id", "TEXT"); err != nil {
		_ = database.Close()
		return nil, err
	}
	return &Store{db: database}, nil
}

// CancelState is the closed durable outcome vocabulary for cancellation.
type CancelState string

const (
	CancelStateCancelled CancelState = "CANCELLED"
	CancelStateNotFound  CancelState = "NOT_FOUND"
	CancelStateUnknown   CancelState = "UNKNOWN"
	// CancelStateRejectedThrottled is a broker answer the edge may re-send: a KIS
	// per-second throttle (HTTP 500, rt_cd 1, msg_cd EGW00201) means the broker
	// did not process the cancel.
	CancelStateRejectedThrottled CancelState = "REJECTED_THROTTLED"
)

func (state CancelState) Valid() bool {
	return state == CancelStateCancelled || state == CancelStateNotFound || state == CancelStateUnknown || state == CancelStateRejectedThrottled
}

// CancelReceipt is the replayable result of cancelling an accepted command.
// Rejection is response-only evidence: the cancel_attempts columns are fixed
// and can never persist it.
type CancelReceipt struct {
	CommandID  string                                `json:"command_id"`
	State      CancelState                           `json:"state"`
	ErrorCode  string                                `json:"error_code,omitempty"`
	RecordedAt string                                `json:"recorded_at"`
	Rejection  *executioncontracts.BrokerRejectionV1 `json:"rejection,omitempty"`
}

// CancelTarget carries only the immutable facts needed to construct a cancel
// request. It is not a second command representation.
type CancelTarget struct {
	CommandID            string
	AccountScope         string
	BrokerOrderID        string
	KRXForwardOrderOrgNo string
	Side                 string
	StockCode            string
	Quantity             string
	Price                string
}

// CancelAttempt is one durable broker cancel attempt. AttemptNo orders the
// attempts of one command. A later attempt can exist only after the earlier
// one was recorded as REJECTED_THROTTLED, and no command gets more than
// maxCancelAttempts broker sends.
type CancelAttempt struct {
	CancelReceipt
	AttemptNo int
}

// maxCancelAttempts caps broker cancel sends per command, the first included.
const maxCancelAttempts = 3

// FindLatestCancelAttempt returns the command's highest-numbered attempt.
func (store *Store) FindLatestCancelAttempt(ctx context.Context, commandID string) (CancelAttempt, bool, error) {
	return store.findCancelAttempt(ctx, commandID, 0)
}

// findCancelAttempt returns one attempt, or the latest when attemptNo is 0.
func (store *Store) findCancelAttempt(ctx context.Context, commandID string, attemptNo int) (CancelAttempt, bool, error) {
	if store == nil || store.db == nil {
		return CancelAttempt{}, false, errors.New("store unavailable")
	}
	query := `SELECT command_id, attempt_no, state, COALESCE(error_code, ''), recorded_at FROM cancel_attempts WHERE command_id = ? ORDER BY attempt_no DESC LIMIT 1`
	args := []any{commandID}
	if attemptNo > 0 {
		query = `SELECT command_id, attempt_no, state, COALESCE(error_code, ''), recorded_at FROM cancel_attempts WHERE command_id = ? AND attempt_no = ?`
		args = append(args, attemptNo)
	}
	var attempt CancelAttempt
	err := store.db.QueryRowContext(ctx, query, args...).
		Scan(&attempt.CommandID, &attempt.AttemptNo, &attempt.State, &attempt.ErrorCode, &attempt.RecordedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return CancelAttempt{}, false, nil
	}
	if err != nil || !validCancelReceipt(attempt.CancelReceipt) {
		if err == nil {
			err = errors.New("invalid stored cancel receipt")
		}
		return CancelAttempt{}, false, err
	}
	return attempt, true, nil
}

// CancelCandidate is a canary command whose cancel is not confirmed. Attempts
// counts the cancel attempts the edge recorded for it. Zero means the broker
// was never asked, so a carry-over cancel for it is the first send.
type CancelCandidate struct {
	CommandID    string
	AccountScope string
	PlacedAt     string
	Attempts     int
	LatestState  string
	// Disposition is the effective place disposition: ACCEPTED, or UNKNOWN for a
	// place whose outcome was never settled.
	Disposition string
	// ResolvedByEvidence is true when a command-scoped resolution, not the place
	// receipt, established the broker order. Such an order is never cancellable by
	// the edge, so it blocks placement but is never sent.
	ResolvedByEvidence bool
}

// CancelCandidates lists commands on one KIS mock scope, with the given
// command-ID prefix, that were accepted and whose latest cancel is absent,
// UNKNOWN or REJECTED_THROTTLED. A confirmed cancel is never listed. The list
// is read-only; the caller applies the placement-window bound.
func (store *Store) CancelCandidates(ctx context.Context, scope, prefix string) ([]CancelCandidate, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("store unavailable")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT c.command_id, c.account_scope, c.recorded_at,
			(SELECT COUNT(*) FROM cancel_attempts a WHERE a.command_id = c.command_id),
			COALESCE(l.state, ''), r.command_id IS NOT NULL, COALESCE(r.disposition, c.disposition)
		FROM commands c
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		LEFT JOIN cancel_attempts l ON l.command_id = c.command_id
			AND l.attempt_no = (SELECT MAX(m.attempt_no) FROM cancel_attempts m WHERE m.command_id = c.command_id)
		WHERE c.account_scope = ?
			AND ((COALESCE(r.disposition, c.disposition) = 'ACCEPTED'
				AND COALESCE(CASE WHEN r.command_id IS NULL THEN c.broker_order_id ELSE r.broker_order_id END, '') <> '')
				OR (r.command_id IS NULL AND c.disposition = 'UNKNOWN'))
			AND substr(c.command_id, 1, ?) = ?
			AND (l.command_id IS NULL OR l.state IN ('UNKNOWN', 'REJECTED_THROTTLED'))
		ORDER BY c.recorded_at, c.command_id`, scope, len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := []CancelCandidate{}
	for rows.Next() {
		var candidate CancelCandidate
		var resolved int
		if err := rows.Scan(&candidate.CommandID, &candidate.AccountScope, &candidate.PlacedAt, &candidate.Attempts, &candidate.LatestState, &resolved, &candidate.Disposition); err != nil {
			return nil, err
		}
		candidate.ResolvedByEvidence = resolved == 1
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// FindCancelTarget returns only orders whose effective disposition is
// ACCEPTED and whose broker order ID is known. KIS resolution rows from older
// releases can contain heuristic matches and cannot authorize cancellation.
func (store *Store) FindCancelTarget(ctx context.Context, commandID string) (CancelTarget, bool, error) {
	if store == nil || store.db == nil {
		return CancelTarget{}, false, errors.New("store unavailable")
	}
	var target CancelTarget
	var krxForwardOrderOrgNo, side, stockCode, quantity, price sql.NullString
	err := store.db.QueryRowContext(ctx, `
		SELECT c.command_id, c.account_scope,
			CASE WHEN r.command_id IS NULL THEN c.broker_order_id ELSE r.broker_order_id END,
			c.krx_fwdg_ord_orgno, x.side, x.stock_code, x.quantity, x.price
		FROM commands c
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		LEFT JOIN command_contexts x ON x.command_id = c.command_id
		WHERE c.command_id = ? AND COALESCE(r.disposition, c.disposition) = 'ACCEPTED'
			AND (r.command_id IS NULL OR COALESCE(c.account_scope, '') NOT IN (?, ?))
			AND COALESCE(CASE WHEN r.command_id IS NULL THEN c.broker_order_id ELSE r.broker_order_id END, '') <> ''
	`, commandID, executioncontracts.AccountScopeKISMock, executioncontracts.AccountScopeKISMockUS).Scan(&target.CommandID, &target.AccountScope, &target.BrokerOrderID, &krxForwardOrderOrgNo, &side, &stockCode, &quantity, &price)
	if errors.Is(err, sql.ErrNoRows) {
		return CancelTarget{}, false, nil
	}
	if err != nil {
		return CancelTarget{}, false, err
	}
	if krxForwardOrderOrgNo.Valid {
		target.KRXForwardOrderOrgNo = krxForwardOrderOrgNo.String
	}
	if side.Valid {
		target.Side = side.String
	}
	if stockCode.Valid {
		target.StockCode = stockCode.String
	}
	if quantity.Valid {
		target.Quantity = quantity.String
	}
	if price.Valid {
		target.Price = price.String
	}
	return target, true, nil
}

// ReserveCancel commits the UNKNOWN pre-send marker for one attempt. A marker
// that already exists for that attempt is returned unchanged, so duplicate
// callers cannot send a second request for it.
func (store *Store) ReserveCancel(ctx context.Context, attempt CancelAttempt) (CancelAttempt, bool, error) {
	if store == nil || store.db == nil || !validCancelReceipt(attempt.CancelReceipt) || attempt.State != CancelStateUnknown || attempt.AttemptNo < 1 {
		return CancelAttempt{}, false, errors.New("invalid cancel receipt")
	}
	result, err := store.db.ExecContext(ctx, `
		INSERT INTO cancel_attempts (command_id, attempt_no, state, error_code, recorded_at)
		SELECT c.command_id, ?, ?, NULLIF(?, ''), ? FROM commands c
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		WHERE c.command_id = ? AND COALESCE(r.disposition, c.disposition) = 'ACCEPTED'
			AND COALESCE(CASE WHEN r.command_id IS NULL THEN c.broker_order_id ELSE r.broker_order_id END, '') <> ''
		ON CONFLICT(command_id, attempt_no) DO NOTHING
	`, attempt.AttemptNo, attempt.State, attempt.ErrorCode, attempt.RecordedAt, attempt.CommandID)
	if err != nil {
		return CancelAttempt{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return CancelAttempt{}, false, err
	}
	if inserted == 1 {
		return attempt, true, nil
	}
	existing, found, err := store.findCancelAttempt(ctx, attempt.CommandID, attempt.AttemptNo)
	if err != nil {
		return CancelAttempt{}, false, err
	}
	if !found {
		return CancelAttempt{}, false, errors.New("cancel target disappeared")
	}
	return existing, false, nil
}

// FinalizeCancel records the broker answer for a reserved attempt. Only a
// pending UNKNOWN attempt can change, so a finalized attempt is never rewritten.
func (store *Store) FinalizeCancel(ctx context.Context, attempt CancelAttempt) (CancelAttempt, error) {
	if store == nil || store.db == nil || !validCancelReceipt(attempt.CancelReceipt) || attempt.AttemptNo < 1 {
		return CancelAttempt{}, errors.New("invalid cancel receipt")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE cancel_attempts SET state = ?, error_code = NULLIF(?, ''), recorded_at = ? WHERE command_id = ? AND attempt_no = ? AND state = 'UNKNOWN'`, attempt.State, attempt.ErrorCode, attempt.RecordedAt, attempt.CommandID, attempt.AttemptNo)
	if err != nil {
		return CancelAttempt{}, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return CancelAttempt{}, err
	}
	if updated == 1 {
		return attempt, nil
	}
	existing, found, err := store.findCancelAttempt(ctx, attempt.CommandID, attempt.AttemptNo)
	if err != nil {
		return CancelAttempt{}, err
	}
	if !found {
		return CancelAttempt{}, errors.New("cancel receipt missing")
	}
	return existing, nil
}

func validCancelReceipt(receipt CancelReceipt) bool {
	return validCommandID(receipt.CommandID) && receipt.State.Valid() && receipt.RecordedAt != ""
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

// StoreWitness persists an immutable live intent without ever constructing a
// broker request. command_id is the durable idempotency key and witness id.
func (store *Store) StoreWitness(ctx context.Context, receipt WitnessReceipt, command executioncontracts.ExecutionCommandV1) (WitnessReceipt, bool, error) {
	if store == nil || store.db == nil || receipt.Mode != shadowWitnessMode ||
		receipt.WitnessID != command.CommandID || receipt.CommandID != command.CommandID ||
		receipt.RecordedAt == "" || ValidateKISLiveWitness(command) != "" {
		return WitnessReceipt{}, false, errors.New("invalid witness")
	}
	result, err := store.db.ExecContext(ctx, `
		INSERT INTO kis_live_witnesses (
			command_id, witness_id, schema_version, account_scope, side, stock_code,
			quantity, price, order_type, issued_at, phase, received_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'shadow', ?)
		ON CONFLICT(command_id) DO NOTHING
	`, command.CommandID, receipt.WitnessID, command.SchemaVersion, command.AccountScope,
		command.Side, command.StockCode, command.Quantity, command.Price, command.OrderType,
		command.IssuedAt, receipt.RecordedAt)
	if err != nil {
		return WitnessReceipt{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return WitnessReceipt{}, false, err
	}
	if inserted == 1 {
		return receipt, true, nil
	}
	existing, found, err := store.FindWitness(ctx, command.CommandID)
	if err != nil || !found {
		if err == nil {
			err = errors.New("witness disappeared after command_id conflict")
		}
		return WitnessReceipt{}, false, err
	}
	return existing, false, nil
}

func (store *Store) FindWitness(ctx context.Context, commandID string) (WitnessReceipt, bool, error) {
	if store == nil || store.db == nil {
		return WitnessReceipt{}, false, errors.New("store unavailable")
	}
	var receipt WitnessReceipt
	err := store.db.QueryRowContext(ctx, `
		SELECT witness_id, command_id, received_at, phase
		FROM kis_live_witnesses WHERE command_id = ?
	`, commandID).Scan(&receipt.WitnessID, &receipt.CommandID, &receipt.RecordedAt, &receipt.Mode)
	if errors.Is(err, sql.ErrNoRows) {
		return WitnessReceipt{}, false, nil
	}
	if err != nil || receipt.Mode != shadowWitnessMode || receipt.CommandID != commandID || receipt.WitnessID == "" || receipt.RecordedAt == "" {
		if err == nil {
			err = errors.New("invalid stored witness")
		}
		return WitnessReceipt{}, false, err
	}
	return receipt, true, nil
}

// AttachWitnessEcho is one-shot. Python's replay safety remains its own
// ledger; the edge rejects a duplicate so audit evidence cannot be rewritten.
func (store *Store) AttachWitnessEcho(ctx context.Context, commandID string, echo WitnessEcho, echoAt string) (WitnessReceipt, string, error) {
	if store == nil || store.db == nil {
		return WitnessReceipt{}, "", errors.New("store unavailable")
	}
	if !validCommandID(commandID) || !validWitnessEcho(echo) || echoAt == "" {
		return WitnessReceipt{}, ErrorInvalidCommand, nil
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE kis_live_witnesses
		SET echo_at = ?, broker_order_id = ?, broker_rt_cd = ?, broker_msg_cd = ?, broker_msg1 = ?, broker_received_at = ?
		WHERE command_id = ? AND echo_at IS NULL
	`, echoAt, echo.ODNO, echo.RTCode, echo.MessageCode, echo.Message, echo.ReceivedAt, commandID)
	if err != nil {
		return WitnessReceipt{}, "", err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return WitnessReceipt{}, "", err
	}
	if updated == 1 {
		receipt, found, err := store.FindWitness(ctx, commandID)
		if err != nil || !found {
			if err == nil {
				err = errors.New("witness disappeared after echo")
			}
			return WitnessReceipt{}, "", err
		}
		return receipt, "", nil
	}
	var echoPresent sql.NullString
	err = store.db.QueryRowContext(ctx, `SELECT echo_at FROM kis_live_witnesses WHERE command_id = ?`, commandID).Scan(&echoPresent)
	if errors.Is(err, sql.ErrNoRows) {
		return WitnessReceipt{}, "witness_not_found", nil
	}
	if err != nil {
		return WitnessReceipt{}, "", err
	}
	return WitnessReceipt{}, "echo_already_recorded", nil
}

func (store *Store) MissingWitnesses(ctx context.Context) ([]WitnessReceipt, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("store unavailable")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT witness_id, command_id, received_at, phase
		FROM kis_live_witnesses WHERE echo_at IS NULL ORDER BY received_at, command_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var witnesses []WitnessReceipt
	for rows.Next() {
		var witness WitnessReceipt
		if err := rows.Scan(&witness.WitnessID, &witness.CommandID, &witness.RecordedAt, &witness.Mode); err != nil {
			return nil, err
		}
		witnesses = append(witnesses, witness)
	}
	return witnesses, rows.Err()
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
		command.CommandID != receipt.CommandID ||
		(command.AccountScope != executioncontracts.AccountScopeKISMock &&
			command.AccountScope != executioncontracts.AccountScopeKISMockUS &&
			command.AccountScope != executioncontracts.AccountScopeAlpacaPaperCrypto) {
		return executioncontracts.ExecutionReceiptV1{}, false, errors.New("invalid pending receipt")
	}
	clientOrderID := ""
	if command.AccountScope == executioncontracts.AccountScopeAlpacaPaperCrypto {
		// Persist the mapping before the send boundary. This marks records whose
		// client_order_id is actually known to have been sent to Alpaca; legacy
		// rows without it must remain unresolved rather than be guessed absent.
		clientOrderID = command.CommandID
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
			INSERT INTO command_contexts (command_id, side, stock_code, quantity, price, sent_at, client_order_id)
			VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''))
		`, command.CommandID, command.Side, command.StockCode, command.Quantity, command.Price, receipt.RecordedAt, clientOrderID); err != nil {
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
	// ClientOrderID is populated only for Alpaca records written after the
	// backend began sending it. Its absence is intentionally not evidence of
	// broker absence for historical rows.
	ClientOrderID string
}

// PendingKISMockResolutions returns only unresolved UNKNOWN records for the
// domestic and US KIS mock scopes. It never changes a receipt.
func (store *Store) PendingKISMockResolutions(ctx context.Context) ([]PendingResolution, error) {
	return store.pendingResolutions(ctx,
		executioncontracts.AccountScopeKISMock,
		executioncontracts.AccountScopeKISMockUS,
	)
}

// PendingAlpacaPaperCryptoResolutions returns only UNKNOWN Alpaca receipts
// whose final evidence, if any, must come from Alpaca's client-order-id read.
func (store *Store) PendingAlpacaPaperCryptoResolutions(ctx context.Context) ([]PendingResolution, error) {
	return store.pendingResolutions(ctx, executioncontracts.AccountScopeAlpacaPaperCrypto)
}

func (store *Store) pendingResolutions(ctx context.Context, scopes ...string) ([]PendingResolution, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("store unavailable")
	}
	if len(scopes) == 0 {
		return nil, errors.New("resolution scope missing")
	}
	placeholders := make([]string, len(scopes))
	arguments := make([]any, len(scopes))
	for index, scope := range scopes {
		placeholders[index] = "?"
		arguments[index] = scope
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT c.schema_version, c.command_id, c.disposition, c.broker_order_id, c.error_code, c.recorded_at,
			c.account_scope, x.side, x.stock_code, x.quantity, x.price, x.sent_at, x.client_order_id
		FROM commands c
		LEFT JOIN command_contexts x ON x.command_id = c.command_id
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		WHERE c.disposition = 'UNKNOWN' AND c.account_scope IN (`+strings.Join(placeholders, ", ")+`) AND r.command_id IS NULL
		ORDER BY c.id
	`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PendingResolution
	for rows.Next() {
		item, err := scanPendingResolution(rows)
		if err != nil {
			return nil, err
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

// FindPendingResolution returns the still-unresolved UNKNOWN record for one
// command ID, including its immutable send facts. A conclusive or missing
// command is indistinguishable here and returns found=false; callers that need
// the effective receipt regardless of state use Find first.
func (store *Store) FindPendingResolution(ctx context.Context, commandID string) (PendingResolution, bool, error) {
	if store == nil || store.db == nil {
		return PendingResolution{}, false, errors.New("store unavailable")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT c.schema_version, c.command_id, c.disposition, c.broker_order_id, c.error_code, c.recorded_at,
			c.account_scope, x.side, x.stock_code, x.quantity, x.price, x.sent_at, x.client_order_id
		FROM commands c
		LEFT JOIN command_contexts x ON x.command_id = c.command_id
		LEFT JOIN command_resolutions r ON r.command_id = c.command_id
		WHERE c.command_id = ? AND c.disposition = 'UNKNOWN' AND r.command_id IS NULL
	`, commandID)
	if err != nil {
		return PendingResolution{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return PendingResolution{}, false, rows.Err()
	}
	item, err := scanPendingResolution(rows)
	if err != nil {
		return PendingResolution{}, false, err
	}
	return item, true, rows.Err()
}

func scanPendingResolution(rows *sql.Rows) (PendingResolution, error) {
	var item PendingResolution
	var brokerOrderID, errorCode, side, stockCode, quantity, price, sentAt, clientOrderID sql.NullString
	if err := rows.Scan(&item.Receipt.SchemaVersion, &item.Receipt.CommandID, &item.Receipt.Disposition,
		&brokerOrderID, &errorCode, &item.Receipt.RecordedAt, &item.AccountScope,
		&side, &stockCode, &quantity, &price, &sentAt, &clientOrderID); err != nil {
		return PendingResolution{}, err
	}
	if brokerOrderID.Valid {
		item.Receipt.BrokerOrderID = brokerOrderID.String
	}
	if errorCode.Valid {
		item.Receipt.ErrorCode = errorCode.String
	}
	if clientOrderID.Valid {
		item.ClientOrderID = clientOrderID.String
	}
	if !validReceipt(item.Receipt) {
		return PendingResolution{}, errors.New("invalid stored receipt")
	}
	item.ContextPresent = side.Valid && stockCode.Valid && quantity.Valid && price.Valid && sentAt.Valid
	if item.ContextPresent {
		item.Side, item.StockCode = side.String, stockCode.String
		item.Quantity, item.Price = quantity.String, price.String
		var parseErr error
		item.SentAt, parseErr = time.Parse(time.RFC3339Nano, sentAt.String)
		if parseErr != nil {
			return PendingResolution{}, errors.New("invalid stored command context")
		}
	} else {
		var parseErr error
		item.SentAt, parseErr = time.Parse(time.RFC3339Nano, item.Receipt.RecordedAt)
		if parseErr != nil {
			return PendingResolution{}, errors.New("invalid stored receipt")
		}
	}
	return item, nil
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
func (store *Store) Finalize(ctx context.Context, receipt executioncontracts.ExecutionReceiptV1, krxForwardOrderOrgNo string) (executioncontracts.ExecutionReceiptV1, error) {
	if store == nil || store.db == nil {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("store unavailable")
	}
	if !validReceipt(receipt) {
		return executioncontracts.ExecutionReceiptV1{}, errors.New("invalid receipt")
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE commands
		SET schema_version = ?, disposition = ?, broker_order_id = NULLIF(?, ''),
			error_code = NULLIF(?, ''), recorded_at = ?, phase = 'final',
			krx_fwdg_ord_orgno = NULLIF(?, '')
		WHERE command_id = ? AND phase = 'pending'
	`, receipt.SchemaVersion, receipt.Disposition, receipt.BrokerOrderID, receipt.ErrorCode, receipt.RecordedAt, krxForwardOrderOrgNo, receipt.CommandID)
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

// migrateCancelAttempts rebuilds a pre-attempt-number cancel_attempts table in
// place. Each legacy row becomes attempt 1 unchanged, so an existing UNKNOWN
// marker keeps its send-boundary meaning.
func migrateCancelAttempts(database *sql.DB) error {
	rows, err := database.Query("PRAGMA table_info(cancel_attempts)")
	if err != nil {
		return err
	}
	current := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == "attempt_no" {
			current = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if current {
		return nil
	}
	transaction, err := database.Begin()
	if err != nil {
		return err
	}
	statements := []string{
		`ALTER TABLE cancel_attempts RENAME TO cancel_attempts_legacy`,
		`CREATE TABLE cancel_attempts (
			command_id TEXT NOT NULL REFERENCES commands(command_id),
			attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
			state TEXT NOT NULL CHECK (state IN ('CANCELLED', 'NOT_FOUND', 'UNKNOWN', 'REJECTED_THROTTLED')),
			error_code TEXT,
			recorded_at TEXT NOT NULL,
			PRIMARY KEY (command_id, attempt_no)
		)`,
		`INSERT INTO cancel_attempts (command_id, attempt_no, state, error_code, recorded_at)
			SELECT command_id, 1, state, error_code, recorded_at FROM cancel_attempts_legacy`,
		`DROP TABLE cancel_attempts_legacy`,
	}
	for _, statement := range statements {
		if _, err := transaction.Exec(statement); err != nil {
			_ = transaction.Rollback()
			return err
		}
	}
	return transaction.Commit()
}
