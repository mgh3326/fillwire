package kismockedge

import (
	"context"
	"math/big"
	"sort"
	"strings"
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

// OverseasOrderHistoryReader is an optional extension of the original
// domestic reader contract. Resolver requires it before it can make any
// conclusion for kis_mock_us; a domestic-only reader is intentionally not
// treated as evidence for an overseas receipt.
type OverseasOrderHistoryReader interface {
	OverseasOrderHistory(context.Context, string) ([]kismockread.OverseasOrder, error)
}

// AlpacaOrderEvidence is the minimal fact needed to promote an UNKNOWN Alpaca
// receipt. It carries no order payload, price, quantity, or credentials.
type AlpacaOrderEvidence struct {
	BrokerOrderID string
}

// AlpacaOrderReader is read-only, client-order-id evidence. A false found
// value means Alpaca completed the lookup and returned no such order; errors
// leave the original receipt UNKNOWN.
type AlpacaOrderReader interface {
	OrderByClientOrderID(context.Context, string) (AlpacaOrderEvidence, bool, error)
}

// Resolver turns only proven KIS mock or Alpaca paper UNKNOWN receipts into a
// conclusion. Read errors, ambiguous evidence, and records still inside the
// grace window are all deliberately retained as UNKNOWN.
type Resolver struct {
	Store        *Store
	Reader       OrderHistoryReader
	AlpacaReader AlpacaOrderReader
	Now          func() time.Time
	Grace        time.Duration
	MatchWindow  time.Duration
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
	kisPending, err := resolver.Store.PendingKISMockResolutions(ctx)
	if err != nil {
		return ResolutionResult{}, err
	}
	alpacaPending, err := resolver.Store.PendingAlpacaPaperCryptoResolutions(ctx)
	if err != nil {
		return ResolutionResult{}, err
	}
	if len(kisPending) == 0 && len(alpacaPending) == 0 {
		return ResolutionResult{}, nil
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
	result, err := resolver.resolveKIS(ctx, kisPending, now, grace, window)
	if err != nil {
		return result, err
	}
	return resolver.resolveAlpaca(ctx, alpacaPending, now, grace, result)
}

func (resolver Resolver) resolveKIS(
	ctx context.Context,
	pending []PendingResolution,
	now func() time.Time,
	grace, window time.Duration,
) (ResolutionResult, error) {
	result := ResolutionResult{}
	if len(pending) == 0 {
		return result, nil
	}
	if resolver.Reader == nil {
		return ResolutionResult{Unresolved: len(pending), ReadFailures: 1}, nil
	}
	type evidenceDay struct {
		Scope string
		Day   string
	}
	byDay := make(map[evidenceDay][]PendingResolution)
	for _, item := range pending {
		key := evidenceDay{Scope: item.AccountScope, Day: kisTradingDay(item.SentAt)}
		byDay[key] = append(byDay[key], item)
	}
	days := make([]evidenceDay, 0, len(byDay))
	for day := range byDay {
		days = append(days, day)
	}
	sort.Slice(days, func(left, right int) bool {
		if days[left].Scope == days[right].Scope {
			return days[left].Day < days[right].Day
		}
		return days[left].Scope < days[right].Scope
	})
	for _, day := range days {
		items := byDay[day]
		switch day.Scope {
		case executioncontracts.AccountScopeKISMock:
			orders, readErr := resolver.Reader.DomesticOrderHistory(ctx, day.Day)
			if readErr != nil {
				result.ReadFailures++
				result.Unresolved += len(items)
				continue
			}
			resolved, unresolved, resolveErr := resolver.resolveEvidence(ctx, items, now, grace, len(orders), func(item PendingResolution) []string {
				matches := matchingOrders(item, orders, window)
				return domesticMatchIDs(matches)
			})
			if resolveErr != nil {
				return result, resolveErr
			}
			result.Resolved = append(result.Resolved, resolved...)
			result.Unresolved += unresolved
		case executioncontracts.AccountScopeKISMockUS:
			overseasReader, available := resolver.Reader.(OverseasOrderHistoryReader)
			if !available {
				// There is no evidence source capable of proving a US conclusion.
				result.ReadFailures++
				result.Unresolved += len(items)
				continue
			}
			orders, readErr := overseasReader.OverseasOrderHistory(ctx, day.Day)
			if readErr != nil {
				result.ReadFailures++
				result.Unresolved += len(items)
				continue
			}
			resolved, unresolved, resolveErr := resolver.resolveEvidence(ctx, items, now, grace, len(orders), func(item PendingResolution) []string {
				matches := matchingOverseasOrders(item, orders, window)
				return overseasMatchIDs(matches)
			})
			if resolveErr != nil {
				return result, resolveErr
			}
			result.Resolved = append(result.Resolved, resolved...)
			result.Unresolved += unresolved
		default:
			result.ReadFailures++
			result.Unresolved += len(items)
		}
	}
	return result, nil
}

func (resolver Resolver) resolveEvidence(
	ctx context.Context,
	items []PendingResolution,
	now func() time.Time,
	grace time.Duration,
	orderCount int,
	matchingIDs func(PendingResolution) []string,
) ([]executioncontracts.ExecutionReceiptV1, int, error) {
	resolved := make([]executioncontracts.ExecutionReceiptV1, 0, len(items))
	unresolved := 0
	for _, item := range items {
		matches := matchingIDs(item)
		if len(matches) == 1 {
			receipt, err := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionAccepted, matches[0], "", now())
			if err != nil {
				return resolved, unresolved, err
			}
			resolved = append(resolved, receipt)
			continue
		}
		// A legacy receipt has no immutable command facts. An empty, successful
		// day is nevertheless evidence that it was not created; any nonempty
		// day remains UNKNOWN rather than guessing a correspondence.
		absentProven := item.ContextPresent || orderCount == 0
		if len(matches) == 0 && absentProven && !now().Before(item.SentAt.Add(grace)) {
			receipt, err := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionNotCreated, "", ErrorResolvedAbsent, now())
			if err != nil {
				return resolved, unresolved, err
			}
			resolved = append(resolved, receipt)
			continue
		}
		unresolved++
	}
	return resolved, unresolved, nil
}

func (resolver Resolver) resolveAlpaca(
	ctx context.Context,
	pending []PendingResolution,
	now func() time.Time,
	grace time.Duration,
	result ResolutionResult,
) (ResolutionResult, error) {
	if len(pending) == 0 {
		return result, nil
	}
	withClientOrderID := make([]PendingResolution, 0, len(pending))
	for _, item := range pending {
		// A prior release did not write client_order_id. Its command ID alone
		// does not prove that Alpaca received that value, so it must not be
		// declared absent by this new evidence source.
		if !item.ContextPresent || item.ClientOrderID != item.Receipt.CommandID || !validCommandID(item.ClientOrderID) {
			result.Unresolved++
			continue
		}
		withClientOrderID = append(withClientOrderID, item)
	}
	if len(withClientOrderID) == 0 {
		return result, nil
	}
	if resolver.AlpacaReader == nil {
		result.ReadFailures++
		result.Unresolved += len(withClientOrderID)
		return result, nil
	}
	for _, item := range withClientOrderID {
		evidence, found, readErr := resolver.AlpacaReader.OrderByClientOrderID(ctx, item.ClientOrderID)
		if readErr != nil {
			result.ReadFailures++
			result.Unresolved++
			continue
		}
		if found {
			if evidence.BrokerOrderID == "" {
				result.ReadFailures++
				result.Unresolved++
				continue
			}
			receipt, resolveErr := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionAccepted, evidence.BrokerOrderID, "", now())
			if resolveErr != nil {
				return result, resolveErr
			}
			result.Resolved = append(result.Resolved, receipt)
			continue
		}
		if !now().Before(item.SentAt.Add(grace)) {
			receipt, resolveErr := resolver.Store.ResolveUnknown(ctx, item.Receipt.CommandID, executioncontracts.DispositionNotCreated, "", ErrorResolvedAbsent, now())
			if resolveErr != nil {
				return result, resolveErr
			}
			result.Resolved = append(result.Resolved, receipt)
			continue
		}
		result.Unresolved++
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

func domesticMatchIDs(matches []kismockread.DomesticOrder) []string {
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		result = append(result, match.BrokerOrderID)
	}
	return result
}

func matchingOverseasOrders(pending PendingResolution, orders []kismockread.OverseasOrder, window time.Duration) []kismockread.OverseasOrder {
	if !pending.ContextPresent {
		return nil
	}
	matches := make([]kismockread.OverseasOrder, 0, 1)
	for _, order := range orders {
		if order.Side != pending.Side || normalizeKISMockUSSymbol(order.StockCode) != pending.StockCode ||
			!samePositiveInteger(order.Quantity, pending.Quantity) || !samePositiveDecimal(order.Price, pending.Price) ||
			absDuration(order.OrderedAt.Sub(pending.SentAt)) > window {
			continue
		}
		matches = append(matches, order)
	}
	return matches
}

func overseasMatchIDs(matches []kismockread.OverseasOrder) []string {
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		result = append(result, match.BrokerOrderID)
	}
	return result
}

func normalizeKISMockUSSymbol(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "/", ".")
}

func samePositiveInteger(left, right string) bool {
	leftValue, leftOK := new(big.Int).SetString(left, 10)
	rightValue, rightOK := new(big.Int).SetString(right, 10)
	return leftOK && rightOK && leftValue.Sign() > 0 && rightValue.Sign() > 0 && leftValue.Cmp(rightValue) == 0
}

func samePositiveDecimal(left, right string) bool {
	leftValue, leftOK := positiveDecimal(left)
	rightValue, rightOK := positiveDecimal(right)
	return leftOK && rightOK && leftValue.Cmp(rightValue) == 0
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
