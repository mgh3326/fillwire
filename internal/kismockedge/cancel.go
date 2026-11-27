package kismockedge

import (
	"context"
	"errors"
	"sort"
	"time"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
)

var errCancelNotEligible = errors.New("cancel target not eligible")

// cancelResendGap is the minimum spacing between two broker cancel sends for
// one command. It is applied only to a re-send after REJECTED_THROTTLED.
const cancelResendGap = 1100 * time.Millisecond

// CancelBroker is intentionally separate from Broker: adding cancellation
// must not alter the established placement interface or its semantics.
type CancelBroker interface {
	PrepareCancel(context.Context, CancelTarget) (PreparedCancelBroker, string)
}

// PreparedCancelBroker has the same one-shot boundary as a placement send.
type PreparedCancelBroker interface {
	SendCancel(context.Context) CancelBrokerResult
}

type CancelBrokerResult struct {
	State     CancelState
	ErrorCode string
	// Rejection carries the broker's own masked failure fields when the broker
	// answered. It is response evidence and is never persisted.
	Rejection *executioncontracts.BrokerRejectionV1
}

// Cancel returns a durable cancellation result.
//
// Send contract. Each broker cancel send is one attempt with its own
// cancel_attempts row. The row is committed as a pending UNKNOWN marker before
// the send, so a crash after that point can never cause a send to be repeated.
// An attempt is re-sent only when the latest recorded attempt is
// REJECTED_THROTTLED, which the KIS mock broker returns for an EGW00201
// per-second throttle. A re-send is paced at least cancelResendGap after the
// previous attempt and is capped at maxCancelAttempts attempts per command.
// Every other answer (any other error, timeout or unreadable reply) stays
// UNKNOWN and is replayed without a broker call. Placement is never re-sent.
func (service *Service) Cancel(ctx context.Context, commandID string) (receipt CancelReceipt, err error) {
	scope := metricScopeUnknown
	defer func() {
		if metrics := service.metricsSink(); metrics != nil {
			metrics.recordCancel(scope, receipt.State)
		}
	}()
	if !validCommandID(commandID) {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	if service == nil || service.Store == nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), errors.New("store unavailable")
	}
	latest, found, err := service.Store.FindLatestCancelAttempt(ctx, commandID)
	if err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	}
	attemptNo := 1
	if found {
		if latest.State != CancelStateRejectedThrottled || latest.AttemptNo >= maxCancelAttempts {
			return latest.CancelReceipt, nil
		}
		attemptNo = latest.AttemptNo + 1
		service.paceResend(latest.RecordedAt)
	}
	target, found, err := service.Store.FindCancelTarget(ctx, commandID)
	if err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	}
	if !found {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	scope = target.AccountScope
	broker, ok := service.brokerForScope(target.AccountScope).(CancelBroker)
	if !ok || broker == nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	prepared, code := broker.PrepareCancel(ctx, target)
	if code != "" || prepared == nil {
		// No marker is written: nothing was sent, so the command stays
		// eligible and a later carry-over can send it for the first time.
		if code == "" {
			code = ErrorBrokerUnknown
		}
		return service.cancelReceipt(commandID, CancelStateUnknown, code), errors.New("cancel preparation failed")
	}
	pending := CancelAttempt{
		CancelReceipt: service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelSendPending),
		AttemptNo:     attemptNo,
	}
	existing, reserved, err := service.Store.ReserveCancel(ctx, pending)
	if err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	}
	if !reserved {
		return existing.CancelReceipt, nil
	}

	// Nothing may occur between this committed marker and the broker send.
	if metrics := service.metricsSink(); metrics != nil {
		metrics.recordBrokerRequest(scope, metricKindCancel)
	}
	result := prepared.SendCancel(ctx)
	if !result.State.Valid() {
		result.State = CancelStateUnknown
	}
	if result.State == CancelStateNotFound && result.ErrorCode == "" {
		result.ErrorCode = ErrorCancelNotFound
	}
	if result.State == CancelStateUnknown && result.ErrorCode == "" {
		result.ErrorCode = ErrorBrokerUnknown
	}
	final := service.cancelReceipt(commandID, result.State, result.ErrorCode)
	final.Rejection = result.Rejection
	stored, err := service.Store.FinalizeCancel(ctx, CancelAttempt{CancelReceipt: final, AttemptNo: attemptNo})
	return stored.CancelReceipt, err
}

// paceResend waits until cancelResendGap has passed since the previous attempt.
// An unparseable timestamp waits the full gap, so pacing never fails open.
func (service *Service) paceResend(lastRecordedAt string) {
	wait := cancelResendGap
	if last, err := time.Parse(time.RFC3339Nano, lastRecordedAt); err == nil {
		now := time.Now
		if service != nil && service.Now != nil {
			now = service.Now
		}
		wait = cancelResendGap - now().Sub(last)
	}
	if wait > 0 {
		sleep := time.Sleep
		if service != nil && service.Sleep != nil {
			sleep = service.Sleep
		}
		sleep(wait)
	}
}

// CancelCarryOverCandidates lists the unconfirmed canary cancels for one KIS
// mock scope whose placement time falls in [from, before). Commands never sent
// to the broker come first, then the rest, oldest first within each group. It
// is read-only.
func (service *Service) CancelCarryOverCandidates(ctx context.Context, scope, prefix string, from, before time.Time) ([]CancelCandidate, error) {
	if service == nil || service.Store == nil {
		return nil, errors.New("store unavailable")
	}
	all, err := service.Store.CancelCandidates(ctx, scope, prefix)
	if err != nil {
		return nil, err
	}
	kept := []CancelCandidate{}
	for _, candidate := range all {
		placed, err := time.Parse(time.RFC3339Nano, candidate.PlacedAt)
		if err != nil {
			return nil, errors.New("invalid stored placement time")
		}
		if !placed.Before(from) && placed.Before(before) {
			kept = append(kept, candidate)
		}
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].Actionable() != kept[j].Actionable() {
			return kept[i].Actionable()
		}
		return kept[i].Attempts == 0 && kept[j].Attempts != 0
	})
	return kept, nil
}

// Actionable reports whether a cancel for this candidate can reach the broker.
// Evidence-resolved orders are never cancelled by the edge. Otherwise a send
// happens only for a never-sent command, or for a REJECTED_THROTTLED one still
// under the attempt cap. Everything else is answered from stored state.
func (candidate CancelCandidate) Actionable() bool {
	if candidate.ResolvedByEvidence {
		return false
	}
	if candidate.Attempts == 0 {
		return true
	}
	return candidate.LatestState == string(CancelStateRejectedThrottled) && candidate.Attempts < maxCancelAttempts
}

func (service *Service) cancelReceipt(commandID string, state CancelState, code string) CancelReceipt {
	now := time.Now
	if service != nil && service.Now != nil {
		now = service.Now
	}
	return CancelReceipt{CommandID: commandID, State: state, ErrorCode: code, RecordedAt: now().UTC().Format(time.RFC3339Nano)}
}
