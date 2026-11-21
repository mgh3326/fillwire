package kismockedge

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

const (
	metricScopeUnknown = "unknown"
	metricKindPlace    = "place"
	metricKindCancel   = "cancel"
)

// Metrics owns one daemon-local Prometheus registry. A private registry keeps
// multiple handlers in tests or embedded processes independent while still
// exposing the standard Go and process collectors on every /metrics endpoint.
// Its labels are deliberately closed enums: command payload values never enter
// a metric label.
type Metrics struct {
	registry       *prometheus.Registry
	commands       *prometheus.CounterVec
	cancels        *prometheus.CounterVec
	brokerRequests *prometheus.CounterVec
	liveCommands   *prometheus.CounterVec
	missingEcho    prometheus.Gauge
	httpDuration   *prometheus.HistogramVec
}

// NewMetrics creates the bounded metrics surface exported by kis-mock-edge.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		commands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "broker_edge_commands_total",
			Help: "Total command processing outcomes by account scope and disposition.",
		}, []string{"scope", "disposition"}),
		cancels: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "broker_edge_cancels_total",
			Help: "Total cancellation outcomes by account scope and state.",
		}, []string{"scope", "state"}),
		brokerRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "broker_edge_broker_requests_total",
			Help: "Total broker requests that crossed a durable send boundary.",
		}, []string{"scope", "kind"}),
		liveCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "edge_commands_total",
			Help: "Total kis_live shadow witness command outcomes.",
		}, []string{"scope", "phase", "outcome"}),
		missingEcho: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "edge_witness_missing_echo",
			Help:        "Current kis_live shadow witnesses without an attached broker echo.",
			ConstLabels: prometheus.Labels{"scope": executioncontracts.AccountScopeKISLive},
		}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "broker_edge_http_request_duration_seconds",
			Help: "HTTP handler latency in seconds.",
		}, []string{"code", "method"}),
	}
	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		metrics.commands,
		metrics.cancels,
		metrics.brokerRequests,
		metrics.liveCommands,
		metrics.missingEcho,
		metrics.httpDuration,
	)

	// Pre-initialize every bounded series so the declared custom metrics are
	// visible before the first command reaches this process.
	for _, scope := range metricScopes() {
		for _, disposition := range metricDispositions() {
			metrics.commands.WithLabelValues(scope, disposition)
		}
		for _, state := range metricCancelStates() {
			metrics.cancels.WithLabelValues(scope, state)
		}
		for _, kind := range []string{metricKindPlace, metricKindCancel} {
			metrics.brokerRequests.WithLabelValues(scope, kind)
		}
	}
	for _, outcome := range []string{"recorded", ErrorScopeDisabled, ErrorInvalidCommand, ErrorStorageFailure} {
		metrics.liveCommands.WithLabelValues(executioncontracts.AccountScopeKISLive, shadowWitnessMode, outcome)
	}
	return metrics
}

func (metrics *Metrics) recordLiveCommand(outcome string) {
	if metrics == nil || metrics.liveCommands == nil {
		return
	}
	switch outcome {
	case "recorded", ErrorScopeDisabled, ErrorInvalidCommand, ErrorStorageFailure:
	default:
		outcome = ErrorInvalidCommand
	}
	metrics.liveCommands.WithLabelValues(executioncontracts.AccountScopeKISLive, shadowWitnessMode, outcome).Inc()
}

func (metrics *Metrics) setMissingWitnesses(count int) {
	if metrics != nil && metrics.missingEcho != nil {
		metrics.missingEcho.Set(float64(count))
	}
}

func (metrics *Metrics) handler() http.Handler {
	if metrics == nil || metrics.registry == nil {
		metrics = NewMetrics()
	}
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}

func (metrics *Metrics) instrument(handler http.Handler) http.Handler {
	if metrics == nil || metrics.httpDuration == nil {
		return handler
	}
	return promhttp.InstrumentHandlerDuration(metrics.httpDuration, handler)
}

func (metrics *Metrics) recordCommand(scope string, disposition executioncontracts.ExecutionDisposition) {
	if metrics != nil && metrics.commands != nil {
		metrics.commands.WithLabelValues(metricScope(scope), metricDisposition(disposition)).Inc()
	}
}

func (metrics *Metrics) recordCancel(scope string, state CancelState) {
	if metrics != nil && metrics.cancels != nil {
		metrics.cancels.WithLabelValues(metricScope(scope), metricCancelState(state)).Inc()
	}
}

func (metrics *Metrics) recordBrokerRequest(scope, kind string) {
	if metrics != nil && metrics.brokerRequests != nil {
		metrics.brokerRequests.WithLabelValues(metricScope(scope), metricRequestKind(kind)).Inc()
	}
}

func metricScopes() []string {
	return []string{
		executioncontracts.AccountScopeKISMock,
		executioncontracts.AccountScopeKISMockUS,
		executioncontracts.AccountScopeAlpacaPaperCrypto,
		metricScopeUnknown,
	}
}

func metricScope(scope string) string {
	switch scope {
	case executioncontracts.AccountScopeKISMock, executioncontracts.AccountScopeKISMockUS, executioncontracts.AccountScopeAlpacaPaperCrypto:
		return scope
	default:
		return metricScopeUnknown
	}
}

func metricDispositions() []string {
	return []string{
		string(executioncontracts.DispositionNotCreated),
		string(executioncontracts.DispositionAccepted),
		string(executioncontracts.DispositionUnknown),
	}
}

func metricDisposition(disposition executioncontracts.ExecutionDisposition) string {
	if disposition.Valid() {
		return string(disposition)
	}
	return string(executioncontracts.DispositionUnknown)
}

func metricCancelStates() []string {
	return []string{string(CancelStateCancelled), string(CancelStateNotFound), string(CancelStateUnknown)}
}

func metricCancelState(state CancelState) string {
	if state.Valid() {
		return string(state)
	}
	return string(CancelStateUnknown)
}

func metricRequestKind(kind string) string {
	if kind == metricKindCancel {
		return metricKindCancel
	}
	return metricKindPlace
}
