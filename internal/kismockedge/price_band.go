package kismockedge

import (
	"context"
	"net/http"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// Quoter performs the closed, GET-only price inquiry for one account scope.
// It is the read-side counterpart of Broker: no implementation may build a
// mutation request.
type Quoter interface {
	PriceBand(ctx context.Context, stockCode string) (kismockread.DomesticPriceBand, string)
}

// KISMockQuoter is the VTS-only price-band implementation. It lazily loads
// the same mock configuration and the same read-only cached token the order
// path uses; it cannot issue, refresh, or write a token.
type KISMockQuoter struct {
	Transport  http.RoundTripper
	LoadConfig ConfigLoader
	// NewGetter builds the Redis GET client for the loaded config. It is a
	// seam for tests; nil uses the real Redis client constructor.
	NewGetter func(kismockread.Config) (kismockread.RedisGetter, string)
}

// PriceBand runs the allowlisted domestic inquire-price read (FHKST01010100).
// Any configuration, token, transport, or parse failure collapses to the
// reader's closed error code so the caller can fail closed.
func (quoter KISMockQuoter) PriceBand(ctx context.Context, stockCode string) (kismockread.DomesticPriceBand, string) {
	if quoter.LoadConfig == nil {
		return kismockread.DomesticPriceBand{}, ErrorStorageFailure
	}
	config, code := quoter.LoadConfig()
	if code != "" {
		return kismockread.DomesticPriceBand{}, code
	}
	newGetter := quoter.NewGetter
	if newGetter == nil {
		newGetter = func(config kismockread.Config) (kismockread.RedisGetter, string) {
			getter, err := kismockread.NewRedisGETClient(config.RedisURL)
			if err != nil {
				return nil, string(err.Code)
			}
			return getter, ""
		}
	}
	getter, code := newGetter(config)
	if code != "" {
		return kismockread.DomesticPriceBand{}, code
	}
	band, readErr := (kismockread.Executor{
		TokenGetter: getter,
		Transport:   quoter.Transport,
	}).DomesticPriceBand(ctx, config, stockCode)
	if readErr != nil {
		return kismockread.DomesticPriceBand{}, string(readErr.Code)
	}
	return band, ""
}

// PriceBand answers the read-only daily band for one domestic mock symbol.
// It deliberately has no durable side effects: nothing is stored, no pending
// marker is written, and no order can be prepared from this path.
func (service *Service) PriceBand(ctx context.Context, scope, stockCode string) (executioncontracts.PriceBandV1, string) {
	band := executioncontracts.PriceBandV1{
		SchemaVersion: executioncontracts.PriceBandV1SchemaVersion,
		StockCode:     stockCode,
	}
	if scope != executioncontracts.AccountScopeKISMock || !allDigits(stockCode, 6) {
		return band, ErrorInvalidCommand
	}
	quoter := service.quoterForScope(scope)
	if quoter == nil {
		return band, ErrorStorageFailure
	}
	read, code := quoter.PriceBand(ctx, stockCode)
	if code != "" {
		return band, code
	}
	band.LastPrice = read.LastPrice
	band.UpperLimit = read.UpperLimit
	band.LowerLimit = read.LowerLimit
	band.BasePrice = read.BasePrice
	return band, ""
}

func (service *Service) quoterForScope(scope string) Quoter {
	if service == nil || service.Quoters == nil {
		return nil
	}
	return service.Quoters[scope]
}
