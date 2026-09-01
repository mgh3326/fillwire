package kismockedge

import (
	"context"
	"net/http"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// ConfigLoader supplies mock-only configuration after the placement gate is
// proven on. It is deliberately lazy so a disabled edge cannot touch Redis.
type ConfigLoader func() (kismockread.Config, string)

// TokenLoader can only load a cached token. No implementation in this package
// has a token issuance, refresh, clear, lock, or write capability.
type TokenLoader interface {
	Load(context.Context, kismockread.Config) (string, string)
}

// Service coordinates scope validation, durable idempotency, and the single
// broker send boundary. Brokers own their backend-specific credential lookup;
// this type owns the one shared fail-honest lifecycle for every scope.
type Service struct {
	Store        *Store
	PlaceEnabled bool
	Brokers      map[string]Broker
	Now          func() time.Time
}

// NewEnvironmentService wires only the existing GET-only token loader and the
// VTS-pinned broker. BROKER_EDGE_MOCK_PLACE_ENABLED must be explicitly true.
func NewEnvironmentService(store *Store, lookup func(string) string, transport http.RoundTripper) *Service {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	return &Service{
		Store:        store,
		PlaceEnabled: strings.TrimSpace(lookup("BROKER_EDGE_MOCK_PLACE_ENABLED")) == "true",
		Brokers: map[string]Broker{
			executioncontracts.AccountScopeKISMock: KISMockBroker{
				Transport: transport,
				LoadConfig: func() (kismockread.Config, string) {
					config, err := kismockread.ConfigFromEnv(lookup)
					if err != nil {
						return kismockread.Config{}, string(err.Code)
					}
					return config, ""
				},
				Tokens: RedisCachedTokenLoader{},
			},
			executioncontracts.AccountScopeKISMockUS: KISMockUSBroker{
				Transport: transport,
				LoadConfig: func() (kismockread.Config, string) {
					config, err := kismockread.ConfigFromEnv(lookup)
					if err != nil {
						return kismockread.Config{}, string(err.Code)
					}
					return config, ""
				},
				Tokens: RedisCachedTokenLoader{},
			},
			executioncontracts.AccountScopeAlpacaPaperCrypto: AlpacaPaperCryptoBroker{
				Transport: transport,
				LoadConfig: func() (AlpacaPaperCryptoConfig, string) {
					return AlpacaPaperCryptoConfigFromEnv(lookup)
				},
			},
		},
	}
}

// Process returns a durable receipt. A matching command_id always wins before
// validation or token loading, so duplicate delivery cannot send again.
func (service *Service) Process(ctx context.Context, command executioncontracts.ExecutionCommandV1) (executioncontracts.ExecutionReceiptV1, error) {
	if !validCommandID(command.CommandID) {
		return service.receipt(command.CommandID, executioncontracts.DispositionNotCreated, ErrorInvalidCommand), nil
	}
	if service == nil || service.Store == nil {
		return service.receipt(command.CommandID, executioncontracts.DispositionNotCreated, ErrorStorageFailure), nil
	}
	if existing, found, err := service.Store.Find(ctx, command.CommandID); err != nil {
		return service.receipt(command.CommandID, executioncontracts.DispositionNotCreated, ErrorStorageFailure), err
	} else if found {
		return existing, nil
	}

	if code := ValidateCommand(command); code != "" {
		return service.storeFinal(ctx, command.CommandID, executioncontracts.DispositionNotCreated, code)
	}
	if !service.PlaceEnabled {
		return service.storeFinal(ctx, command.CommandID, executioncontracts.DispositionNotCreated, ErrorPlaceDisabled)
	}
	if code := ValidateOrderCaps(command); code != "" {
		return service.storeFinal(ctx, command.CommandID, executioncontracts.DispositionNotCreated, code)
	}
	broker := service.brokerForScope(command.AccountScope)
	if broker == nil {
		return service.storeFinal(ctx, command.CommandID, executioncontracts.DispositionNotCreated, ErrorStorageFailure)
	}
	prepared, prepareCode := broker.Prepare(ctx, command)
	if prepareCode != "" || prepared == nil {
		if prepareCode == "" {
			prepareCode = ErrorInvalidCommand
		}
		return service.storeFinal(ctx, command.CommandID, executioncontracts.DispositionNotCreated, prepareCode)
	}

	pending := service.receipt(command.CommandID, executioncontracts.DispositionUnknown, ErrorSendPending)
	existing, reserved, err := service.Store.ReservePending(ctx, pending, command)
	if err != nil {
		return service.receipt(command.CommandID, executioncontracts.DispositionNotCreated, ErrorStorageFailure), err
	}
	if !reserved {
		return existing, nil
	}

	// No local work belongs between ReservePending's commit and this Send call.
	// From this exact point, every non-accepted outcome is UNKNOWN.
	result := prepared.Send(ctx)
	if result.Accepted && result.BrokerOrderID != "" {
		return service.finalize(ctx, command.CommandID, executioncontracts.DispositionAccepted, result.BrokerOrderID, "", result.KRXForwardOrderOrgNo)
	}
	if result.ErrorCode == "" {
		result.ErrorCode = ErrorBrokerUnknown
	}
	return service.finalize(ctx, command.CommandID, executioncontracts.DispositionUnknown, "", result.ErrorCode, "")
}

func (service *Service) brokerForScope(scope string) Broker {
	if service == nil || service.Brokers == nil {
		return nil
	}
	return service.Brokers[scope]
}

func (service *Service) storeFinal(ctx context.Context, commandID string, disposition executioncontracts.ExecutionDisposition, code string) (executioncontracts.ExecutionReceiptV1, error) {
	receipt := service.receipt(commandID, disposition, code)
	stored, _, err := service.Store.StoreFinal(ctx, receipt)
	if err != nil {
		return receipt, err
	}
	return stored, nil
}

func (service *Service) finalize(ctx context.Context, commandID string, disposition executioncontracts.ExecutionDisposition, brokerOrderID, code, krxForwardOrderOrgNo string) (executioncontracts.ExecutionReceiptV1, error) {
	receipt := service.receipt(commandID, disposition, code)
	receipt.BrokerOrderID = brokerOrderID
	stored, err := service.Store.Finalize(ctx, receipt, krxForwardOrderOrgNo)
	if err != nil {
		return receipt, err
	}
	return stored, nil
}

func (service *Service) receipt(commandID string, disposition executioncontracts.ExecutionDisposition, code string) executioncontracts.ExecutionReceiptV1 {
	now := time.Now
	if service != nil && service.Now != nil {
		now = service.Now
	}
	return executioncontracts.ExecutionReceiptV1{
		SchemaVersion: executioncontracts.ExecutionReceiptV1SchemaVersion,
		CommandID:     commandID,
		Disposition:   disposition,
		ErrorCode:     code,
		RecordedAt:    now().UTC().Format(time.RFC3339Nano),
	}
}
