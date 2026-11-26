package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Module owns its resources for one attempt. Health is safe to call concurrently.
type Module interface {
	Name() string
	Run(context.Context) error
	Health() bool
}

type serviceModule struct {
	name  string
	start func(context.Context, func()) error
	ready atomic.Bool
}

func (m *serviceModule) Name() string { return m.name }
func (m *serviceModule) Health() bool { return m.ready.Load() }
func (m *serviceModule) Run(ctx context.Context) error {
	defer m.ready.Store(false)
	return m.start(ctx, func() { m.ready.Store(true) })
}

type moduleStatus struct {
	State    string `json:"state"`
	Restarts uint64 `json:"restarts"`
}

type moduleSlot struct {
	name string
	Module
	mu     sync.Mutex
	status moduleStatus
}

func (s *moduleSlot) snapshot() moduleStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.status
	if status.State == "starting" && s.Health() {
		status.State = "running"
	}
	return status
}

type supervisor struct {
	slots    []*moduleSlot
	logger   *slog.Logger
	retryMin time.Duration
	retryMax time.Duration
}

func newSupervisor(slots []*moduleSlot, logger *slog.Logger) *supervisor {
	for _, slot := range slots {
		slot.status.State = "disabled"
		if slot.Module != nil {
			slot.status.State = "starting"
		}
	}
	return &supervisor{slots: slots, logger: logger, retryMin: time.Second, retryMax: time.Minute}
}

func (s *supervisor) run(ctx context.Context) {
	var workers sync.WaitGroup
	for _, slot := range s.slots {
		if slot.Module != nil {
			workers.Go(func() { s.watch(ctx, slot) })
		}
	}
	workers.Wait()
}

func (s *supervisor) watch(ctx context.Context, slot *moduleSlot) {
	defer func() {
		slot.mu.Lock()
		slot.status.State = "stopped"
		slot.mu.Unlock()
	}()
	delay := s.retryMin
	for ctx.Err() == nil {
		started := time.Now()
		err := runAttempt(func() error { return slot.Run(ctx) })
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) >= s.retryMax {
			delay = s.retryMin
		}
		slot.mu.Lock()
		slot.status.State = "backoff"
		slot.mu.Unlock()
		s.logger.Warn("모듈이 종료되어 다시 시작합니다", "module", slot.Name(), "reason", safeRuntimeError(err), "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		slot.mu.Lock()
		slot.status.Restarts++
		slot.status.State = "starting"
		slot.mu.Unlock()
		delay = min(delay*2, s.retryMax)
	}
}

func runAttempt(run func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("모듈 실행 중 패닉")
		}
	}()
	if err := run(); err != nil {
		return err
	}
	return errors.New("모듈 실행이 종료되었습니다")
}

func (s *supervisor) health(w http.ResponseWriter, _ *http.Request) {
	modules := make(map[string]moduleStatus, len(s.slots))
	code := http.StatusOK
	for _, slot := range s.slots {
		status := slot.snapshot()
		modules[slot.name] = status
		if status.State != "disabled" && status.State != "running" {
			code = http.StatusServiceUnavailable
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Modules map[string]moduleStatus `json:"modules"`
	}{modules})
}
