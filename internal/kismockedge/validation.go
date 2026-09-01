// Package kismockedge implements the deliberately small KIS mock order edge.
package kismockedge

import (
	"math/big"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

const (
	// MaxOrderQuantity and MaxOrderNotionalKRW are fixed process constants.
	// They intentionally have no environment override.
	MaxOrderQuantity    int64 = 100
	MaxOrderNotionalKRW int64 = 1_000_000

	// MaxAlpacaPaperCryptoNotionalUSD is the immutable paper-crypto smoke cap.
	// It is deliberately an integer constant rather than an environment setting.
	MaxAlpacaPaperCryptoNotionalUSD int64 = 10

	ErrorInvalidCommand       = "invalid_command"
	ErrorTickMismatch         = "tick_mismatch"
	ErrorPlaceDisabled        = "place_disabled"
	ErrorOrderLimitExceeded   = "order_limit_exceeded"
	ErrorSendPending          = "send_pending"
	ErrorStorageFailure       = "storage_failure"
	ErrorConfigurationMissing = "configuration_missing"
	ErrorBrokerTimeout        = "broker_timeout"
	ErrorBroker5xx            = "broker_5xx"
	ErrorBrokerUnknown        = "broker_unknown"
)

// ValidateCommand checks the closed command vocabulary before a broker request
// can be prepared. Scope-specific validation never changes Price or Quantity.
func ValidateCommand(command executioncontracts.ExecutionCommandV1) string {
	if command.SchemaVersion != executioncontracts.ExecutionCommandV1SchemaVersion ||
		!validCommandID(command.CommandID) ||
		command.OrderType != "limit" ||
		!validIssuedAt(command.IssuedAt) {
		return ErrorInvalidCommand
	}

	switch command.AccountScope {
	case executioncontracts.AccountScopeKISMock:
		return validateKISMockCommand(command)
	case executioncontracts.AccountScopeAlpacaPaperCrypto:
		return validateAlpacaPaperCryptoCommand(command)
	default:
		return ErrorInvalidCommand
	}
}

func validateKISMockCommand(command executioncontracts.ExecutionCommandV1) string {
	if command.AccountScope != executioncontracts.AccountScopeKISMock ||
		(command.Side != "buy" && command.Side != "sell") || !allDigits(command.StockCode, 6) {
		return ErrorInvalidCommand
	}
	_, validQuantity := positiveInteger(command.Quantity)
	price, validPrice := positiveInteger(command.Price)
	if !validQuantity || !validPrice {
		return ErrorInvalidCommand
	}
	if !TickMatchesKR(price, command.Side) {
		return ErrorTickMismatch
	}
	return ""
}

func validateAlpacaPaperCryptoCommand(command executioncontracts.ExecutionCommandV1) string {
	if command.AccountScope != executioncontracts.AccountScopeAlpacaPaperCrypto ||
		command.Side != "buy" || command.StockCode != AlpacaPaperCryptoSymbolBTCUSD {
		return ErrorInvalidCommand
	}
	_, validQuantity := positiveDecimal(command.Quantity)
	_, validPrice := positiveDecimal(command.Price)
	if !validQuantity || !validPrice {
		return ErrorInvalidCommand
	}
	return ""
}

// ValidateOrderCaps applies scope-specific immutable limits after structural
// validation. It never returns normalized values, so the originally supplied
// decimal strings are the exact values that reach the broker body.
func ValidateOrderCaps(command executioncontracts.ExecutionCommandV1) string {
	switch command.AccountScope {
	case executioncontracts.AccountScopeKISMock:
		return validateKISMockOrderCaps(command)
	case executioncontracts.AccountScopeAlpacaPaperCrypto:
		return validateAlpacaPaperCryptoOrderCaps(command)
	default:
		return ErrorInvalidCommand
	}
}

func validateKISMockOrderCaps(command executioncontracts.ExecutionCommandV1) string {
	quantity, validQuantity := positiveInteger(command.Quantity)
	price, validPrice := positiveInteger(command.Price)
	if !validQuantity || !validPrice {
		return ErrorInvalidCommand
	}
	if quantity.Cmp(big.NewInt(MaxOrderQuantity)) > 0 {
		return ErrorOrderLimitExceeded
	}
	notional := new(big.Int).Mul(price, quantity)
	if notional.Cmp(big.NewInt(MaxOrderNotionalKRW)) > 0 {
		return ErrorOrderLimitExceeded
	}
	return ""
}

func validateAlpacaPaperCryptoOrderCaps(command executioncontracts.ExecutionCommandV1) string {
	quantity, validQuantity := positiveDecimal(command.Quantity)
	price, validPrice := positiveDecimal(command.Price)
	if !validQuantity || !validPrice {
		return ErrorInvalidCommand
	}
	notional := new(big.Rat).Mul(quantity, price)
	if notional.Cmp(new(big.Rat).SetInt64(MaxAlpacaPaperCryptoNotionalUSD)) > 0 {
		return ErrorOrderLimitExceeded
	}
	return ""
}

// GetTickSizeKR ports auto_trader's get_tick_size_kr rule using integer
// arithmetic. The returned value is detached from price and safe to mutate.
func GetTickSizeKR(price *big.Int) *big.Int {
	if price == nil {
		return big.NewInt(1)
	}
	switch {
	case price.Cmp(big.NewInt(2_000)) < 0:
		return big.NewInt(1)
	case price.Cmp(big.NewInt(5_000)) < 0:
		return big.NewInt(5)
	case price.Cmp(big.NewInt(20_000)) < 0:
		return big.NewInt(10)
	case price.Cmp(big.NewInt(50_000)) < 0:
		return big.NewInt(50)
	case price.Cmp(big.NewInt(200_000)) < 0:
		return big.NewInt(100)
	case price.Cmp(big.NewInt(500_000)) < 0:
		return big.NewInt(500)
	default:
		return big.NewInt(1_000)
	}
}

// AdjustTickSizeKR ports auto_trader's adjust_tick_size_kr behavior as a pure
// validation helper. The service compares its result with the original price;
// it never uses this function to rewrite a command.
func AdjustTickSizeKR(price *big.Int, side string) (*big.Int, bool) {
	if price == nil || price.Sign() < 0 || (side != "buy" && side != "sell") {
		return nil, false
	}
	tick := GetTickSizeKR(price)
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.QuoRem(price, tick, remainder)
	adjusted := new(big.Int).Mul(quotient, tick)
	if side == "sell" && remainder.Sign() != 0 {
		adjusted.Add(adjusted, tick)
	}
	if adjusted.Sign() == 0 {
		adjusted.SetInt64(1)
	}
	return adjusted, true
}

// TickMatchesKR says whether the supplied price is already a valid tick. It
// deliberately returns only a verdict, not an adjusted representation.
func TickMatchesKR(price *big.Int, side string) bool {
	adjusted, valid := AdjustTickSizeKR(price, side)
	return valid && price != nil && price.Cmp(adjusted) == 0
}

func validCommandID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func validIssuedAt(value string) bool {
	if strings.TrimSpace(value) != value || value == "" {
		return false
	}
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}

func positiveInteger(value string) (*big.Int, bool) {
	if value == "" || len(value) > 128 {
		return nil, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return nil, false
		}
	}
	parsed, valid := new(big.Int).SetString(value, 10)
	return parsed, valid && parsed.Sign() > 0
}

// positiveDecimal accepts a strict, positive base-10 decimal representation.
// It deliberately rejects signs, exponent notation, whitespace, and incomplete
// fractions so the exact caller-provided string can be forwarded unchanged.
func positiveDecimal(value string) (*big.Rat, bool) {
	if value == "" || len(value) > 128 {
		return nil, false
	}
	dot := -1
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character >= '0' && character <= '9':
		case character == '.' && dot == -1:
			dot = index
		default:
			return nil, false
		}
	}
	if dot == 0 || dot == len(value)-1 {
		return nil, false
	}
	parsed, valid := new(big.Rat).SetString(value)
	return parsed, valid && parsed.Sign() > 0
}

func allDigits(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
