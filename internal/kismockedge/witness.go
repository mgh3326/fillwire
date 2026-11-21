package kismockedge

import (
	"context"
	"errors"
	"math/big"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

const shadowWitnessMode = "shadow"

// WitnessReceipt is the replayable acknowledgement of a kis_live intent. A
// witness is not an execution receipt: this process did not send the order.
type WitnessReceipt struct {
	WitnessID  string `json:"witness_id"`
	CommandID  string `json:"command_id"`
	RecordedAt string `json:"recorded_at"`
	Mode       string `json:"mode"`
}

// WitnessEcho contains the selected, auditable facts from Python's actual
// broker response. It deliberately does not retain an arbitrary payload.
type WitnessEcho struct {
	ODNO        string `json:"ODNO"`
	RTCode      string `json:"rt_cd"`
	MessageCode string `json:"msg_cd"`
	Message     string `json:"msg1"`
	ReceivedAt  string `json:"received_at"`
}

func (service *Service) ProcessWitness(ctx context.Context, command executioncontracts.ExecutionCommandV1) (WitnessReceipt, string, error) {
	if service == nil || service.Store == nil {
		return WitnessReceipt{}, ErrorStorageFailure, errors.New("store unavailable")
	}
	if !service.KISLiveShadowEnabled {
		return WitnessReceipt{}, ErrorScopeDisabled, nil
	}
	if code := ValidateKISLiveWitness(command); code != "" {
		return WitnessReceipt{}, code, nil
	}
	receipt := WitnessReceipt{
		WitnessID:  command.CommandID,
		CommandID:  command.CommandID,
		RecordedAt: service.nowUTC(),
		Mode:       shadowWitnessMode,
	}
	stored, _, err := service.Store.StoreWitness(ctx, receipt, command)
	if err != nil {
		return WitnessReceipt{}, ErrorStorageFailure, err
	}
	return stored, "", nil
}

func (service *Service) AttachWitnessEcho(ctx context.Context, commandID string, echo WitnessEcho) (WitnessReceipt, string, error) {
	if service == nil || service.Store == nil {
		return WitnessReceipt{}, ErrorStorageFailure, errors.New("store unavailable")
	}
	if !validCommandID(commandID) || !validWitnessEcho(echo) {
		return WitnessReceipt{}, ErrorInvalidCommand, nil
	}
	stored, code, err := service.Store.AttachWitnessEcho(ctx, commandID, echo, service.nowUTC())
	if err != nil {
		return WitnessReceipt{}, ErrorStorageFailure, err
	}
	return stored, code, nil
}

func (service *Service) MissingWitnesses(ctx context.Context) ([]WitnessReceipt, error) {
	if service == nil || service.Store == nil {
		return nil, errors.New("store unavailable")
	}
	return service.Store.MissingWitnesses(ctx)
}

func (service *Service) refreshMissingWitnessMetric(ctx context.Context) {
	if service == nil {
		return
	}
	witnesses, err := service.MissingWitnesses(ctx)
	if err == nil {
		service.installMetrics().setMissingWitnesses(len(witnesses))
	}
}

func (service *Service) nowUTC() string {
	now := time.Now
	if service != nil && service.Now != nil {
		now = service.Now
	}
	return now().UTC().Format(time.RFC3339Nano)
}

// ValidateKISLiveWitness deliberately has no dependency on a broker, token,
// URL, host pin, or TR table. Both Korean and US symbol syntax can be
// witnessed because Python owns live policy and broker submission.
func ValidateKISLiveWitness(command executioncontracts.ExecutionCommandV1) string {
	if command.SchemaVersion != executioncontracts.ExecutionCommandV1SchemaVersion ||
		!validCommandID(command.CommandID) || command.AccountScope != executioncontracts.AccountScopeKISLive ||
		(command.Side != "buy" && command.Side != "sell") || command.OrderType != "limit" ||
		!validIssuedAt(command.IssuedAt) ||
		(!allDigits(command.StockCode, 6) && !validKISMockUSSymbol(command.StockCode)) {
		return ErrorInvalidCommand
	}
	if _, valid := positiveInteger(command.Quantity); !valid {
		return ErrorInvalidCommand
	}
	if _, valid := nonNegativeDecimal(command.Price); !valid {
		return ErrorInvalidCommand
	}
	return ""
}

func nonNegativeDecimal(value string) (*big.Rat, bool) {
	if value == "" || len(value) > 128 {
		return nil, false
	}
	dot := -1
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= '0' && character <= '9' {
			continue
		}
		if character == '.' && dot == -1 {
			dot = index
			continue
		}
		return nil, false
	}
	if dot == 0 || dot == len(value)-1 {
		return nil, false
	}
	parsed, valid := new(big.Rat).SetString(value)
	return parsed, valid && parsed.Sign() >= 0
}

func validWitnessEcho(echo WitnessEcho) bool {
	return echo.ReceivedAt != "" && validIssuedAt(echo.ReceivedAt) &&
		safeWitnessText(echo.ODNO) && safeWitnessText(echo.RTCode) &&
		safeWitnessText(echo.MessageCode) && safeWitnessText(echo.Message)
}

func safeWitnessText(value string) bool {
	if len(value) > 1024 {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
