package kismockedge

import (
	"context"
	"errors"
	"time"
)

var errCancelNotEligible = errors.New("cancel target not eligible")

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
}

// Cancel returns a durable cancellation result. A committed UNKNOWN marker is
// the send boundary: a process loss after it cannot cause a later retry to
// retransmit the broker request.
func (service *Service) Cancel(ctx context.Context, commandID string) (CancelReceipt, error) {
	if !validCommandID(commandID) {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	if service == nil || service.Store == nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), errors.New("store unavailable")
	}
	if existing, found, err := service.Store.FindCancelAttempt(ctx, commandID); err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	} else if found {
		return existing, nil
	}
	target, found, err := service.Store.FindCancelTarget(ctx, commandID)
	if err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	}
	if !found {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	broker, ok := service.brokerForScope(target.AccountScope).(CancelBroker)
	if !ok || broker == nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelNotEligible), errCancelNotEligible
	}
	prepared, code := broker.PrepareCancel(ctx, target)
	if code != "" || prepared == nil {
		if code == "" {
			code = ErrorBrokerUnknown
		}
		return service.cancelReceipt(commandID, CancelStateUnknown, code), errors.New("cancel preparation failed")
	}
	pending := service.cancelReceipt(commandID, CancelStateUnknown, ErrorCancelSendPending)
	existing, reserved, err := service.Store.ReserveCancel(ctx, pending)
	if err != nil {
		return service.cancelReceipt(commandID, CancelStateUnknown, ErrorStorageFailure), err
	}
	if !reserved {
		return existing, nil
	}

	// Nothing may occur between this committed marker and the broker send.
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
	return service.Store.FinalizeCancel(ctx, service.cancelReceipt(commandID, result.State, result.ErrorCode))
}

func (service *Service) cancelReceipt(commandID string, state CancelState, code string) CancelReceipt {
	now := time.Now
	if service != nil && service.Now != nil {
		now = service.Now
	}
	return CancelReceipt{CommandID: commandID, State: state, ErrorCode: code, RecordedAt: now().UTC().Format(time.RFC3339Nano)}
}
