package kismockedge

import (
	"context"
	"math/big"
	"sort"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	// DefaultResolutionGrace prevents a just-sent order from being declared
	// absent before the broker's order-history view has had time to settle.
	DefaultResolutionGrace = 10 * time.Minute
	defaultMatchWindow     = 5 * time.Minute
	ErrorResolvedAbsent    = "resolved_absent"
)

// OrderHistoryReader is read-only evidence. Implementations must not issue,
// refresh, write, or clear a token and must use a GET-only broker path.
type OrderHistoryReader interface {
	DomesticOrderHistory(context.Context, string) ([]kismockread.DomesticOrder, error)
}

// Resolver turns only proven KIS mock UNKNOWN receipts into a conclusion.
// Read errors, ambiguous evidence, and records still inside the grace window
// are all deliberately retained as UNKNOWN.
type Resolver struct {
	Store       *Store
	Reader      OrderHistoryReader
	Now         func() time.Time
	Grace       time.Duration
	MatchWindow time.Duration
}

// ResolutionResult reports every receipt that obtained a durable additive
// resolution. It does not include UNKNOWN receipts, whose original state
// remains unchanged.
type ResolutionResult struct {
	Resolved     []executioncontracts.ExecutionReceiptV1
	Unresolved   int
	ReadFailures int
}

// Resolve reconciles every currently unresolved kis_mock receipt. A failed
// daily read is counted but is not an error result: that distinction lets a
// scheduled invoker retry safely while preserving fail-honest semantics.
func (resolver Resolver) Resolve(ctx context.Context) (ResolutionResult, error) {
	if resolver.Store == nil {
		return ResolutionResult{}, errResolverUnavailable
	}
	pending, err := resolver.Store.PendingKISMockResolutions(ctx)
	if err != nil {
		return ResolutionResult{}, err
	}
	if len(pending) == 0 {
		return ResolutionResult{}, nil
	}
	if resolver.Reader == nil {
		return ResolutionResult{Unresolved: len(pending), ReadFailures: 1}, nil
	}
	now := time.Now
	if resolver.Now != nil {
		now = resolver.Now
	}
	grace := resolver.Grace
	if grace == 0 {
		grace = DefaultResolutionGrace
	}
	window := resolver.MatchWindow
	if window == 0 {
		window = defaultMatchWindow
	}
	byDay := make(map[string][]PendingResolution)
	for _, item := range pending {
		byDay[kisTradingDay(item.SentAt)] = append(byDay[kisTradingDay(item.SentAt)], item)
	}
	days := make([]string, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Strings(days)
	result := ResolutionResult{}
	for _, day := range days {
		orders, readErr := resolver.Reader.DomesticOrderHistory(ctx, day)
		if readErr != nil {
			result.ReadFailures++
			result.Unresolved += len(byDay[day])
			continue
		}
		for _, item := range byDay[day] {
			matches := matchingOrders(item, orders, window)
			if len(matches) == 1 {
				receipt, resolveErr := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionAccepted, matches[0].BrokerOrderID, "", now())
				if resolveErr != nil {
					return result, resolveErr
				}
				result.Resolved = append(result.Resolved, receipt)
				continue
			}
			// A legacy receipt has no immutable command facts. An empty, successful
			// day is nevertheless evidence that it was not created; any nonempty
			// day remains UNKNOWN rather than guessing a correspondence.
			absentProven := item.ContextPresent || len(orders) == 0
			if len(matches) == 0 && absentProven && !now().Before(item.SentAt.Add(grace)) {
				receipt, resolveErr := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionNotCreated, "", ErrorResolvedAbsent, now())
				if resolveErr != nil {
					return result, resolveErr
				}
				result.Resolved = append(result.Resolved, receipt)
				continue
			}
			result.Unresolved++
		}
	}
	return result, nil
}

var errResolverUnavailable = &resolverError{}

type resolverError struct{}

func (*resolverError) Error() string { return "resolver unavailable" }

func kisTradingDay(value time.Time) string {
	return value.In(time.FixedZone("KST", 9*60*60)).Format("20060102")
}

func matchingOrders(pending PendingResolution, orders []kismockread.DomesticOrder, window time.Duration) []kismockread.DomesticOrder {
	if !pending.ContextPresent {
		return nil
	}
	matches := make([]kismockread.DomesticOrder, 0, 1)
	for _, order := range orders {
		if order.Side != pending.Side || order.StockCode != pending.StockCode ||
			!samePositiveInteger(order.Quantity, pending.Quantity) || !samePositiveInteger(order.Price, pending.Price) ||
			absDuration(order.OrderedAt.Sub(pending.SentAt)) > window {
			continue
		}
		matches = append(matches, order)
	}
	return matches
}

func samePositiveInteger(left, right string) bool {
	leftValue, leftOK := new(big.Int).SetString(left, 10)
	rightValue, rightOK := new(big.Int).SetString(right, 10)
	return leftOK && rightOK && leftValue.Sign() > 0 && rightValue.Sign() > 0 && leftValue.Cmp(rightValue) == 0
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
