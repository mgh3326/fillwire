package kismockedge

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// RunResolve runs the local, read-evidence-only UNKNOWN resolver. It is kept
// separate from Run so the long-lived placement listener cannot accidentally
// start when an operator intended a bounded reconciliation invocation.
func RunResolve(ctx context.Context, args []string, lookup func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("resolve", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	grace := flags.Duration("grace", DefaultResolutionGrace, "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *grace < 0 {
		writeResolveFailure(stderr, "invalid_input")
		return 2
	}
	config, err := ServerConfigFromEnv(lookup)
	if err != nil {
		writeResolveFailure(stderr, "startup_failed")
		return 1
	}
	store, err := OpenStore(config.SQLitePath)
	if err != nil {
		writeResolveFailure(stderr, "startup_failed")
		return 1
	}
	defer store.Close()
	result, err := (Resolver{
		Store:  store,
		Reader: environmentOrderHistoryReader{lookup: lookup},
		Grace:  *grace,
	}).Resolve(ctx)
	if err != nil {
		writeResolveFailure(stderr, "storage_failure")
		return 1
	}
	_ = json.NewEncoder(stdout).Encode(resolveOutput{Resolved: result.Resolved, Unresolved: result.Unresolved, ReadFailures: result.ReadFailures})
	if result.ReadFailures != 0 {
		return 1
	}
	return 0
}

type resolveOutput struct {
	Resolved     []executioncontracts.ExecutionReceiptV1 `json:"resolved"`
	Unresolved   int                                     `json:"unresolved"`
	ReadFailures int                                     `json:"read_failures"`
}

func writeResolveFailure(stderr io.Writer, code string) {
	if stderr != nil {
		_, _ = io.WriteString(stderr, "kis-mock-edge resolve: "+code+"\n")
	}
}

type environmentOrderHistoryReader struct {
	lookup func(string) string
}

func (reader environmentOrderHistoryReader) DomesticOrderHistory(ctx context.Context, day string) ([]kismockread.DomesticOrder, error) {
	lookup := reader.lookup
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config, configErr := kismockread.ConfigFromEnv(lookup)
	if configErr != nil {
		return nil, configErr
	}
	getter, getterErr := kismockread.NewRedisGETClient(config.RedisURL)
	if getterErr != nil {
		return nil, getterErr
	}
	orders, readErr := (kismockread.Executor{TokenGetter: getter, Now: time.Now}).DomesticOrderHistory(ctx, config, day)
	if readErr != nil {
		return nil, readErr
	}
	return orders, nil
}


// OverseasOrderHistory is the only mock-US evidence reader. It deliberately
// mirrors auto_trader's mock-available daily history TR, not the mock-blocked
// overseas pending-orders inquiry.
func (reader environmentOrderHistoryReader) OverseasOrderHistory(ctx context.Context, day string) ([]kismockread.OverseasOrder, error) {
	lookup := reader.lookup
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config, configErr := kismockread.ConfigFromEnv(lookup)
	if configErr != nil {
		return nil, configErr
	}
	getter, getterErr := kismockread.NewRedisGETClient(config.RedisURL)
	if getterErr != nil {
		return nil, getterErr
	}
	orders, readErr := (kismockread.Executor{TokenGetter: getter, Now: time.Now}).OverseasOrderHistory(ctx, config, day)
	if readErr != nil {
		return nil, readErr
	}
	return orders, nil
}
