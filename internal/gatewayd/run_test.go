package gatewayd

import (
	"context"
	"sync"
	"testing"
)

type countingEnsurer struct {
	mu    sync.Mutex
	calls int
	state EnsureState
}

func (ensurer *countingEnsurer) Ensure(context.Context) (EnsureState, error) {
	ensurer.mu.Lock()
	defer ensurer.mu.Unlock()
	ensurer.calls++
	return ensurer.state, nil
}

func (ensurer *countingEnsurer) callCount() int {
	ensurer.mu.Lock()
	defer ensurer.mu.Unlock()
	return ensurer.calls
}

func TestEnsureConfiguredProvidersOnlyRunsSelectedEnsurers(t *testing.T) {
	mock := &countingEnsurer{state: EnsureStateFresh}
	toss := &countingEnsurer{state: EnsureStateIssued}
	ensureConfiguredProviders(context.Background(), ProviderEnsurers{
		ProviderKISMock: mock,
		ProviderToss:    toss,
	}, nil)
	if mock.callCount() != 1 || toss.callCount() != 1 {
		t.Fatalf("configured calls = mock %d, toss %d", mock.callCount(), toss.callCount())
	}
}
