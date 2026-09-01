package gatewayd

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ensureMetricStateFailed = "failed"

// Metrics owns gatewayd's daemon-local Prometheus registry. Like the edge
// registry, it includes standard Go and process collectors rather than relying
// on the process-global default registry.
type Metrics struct {
	registry       *prometheus.Registry
	ensureOutcomes *prometheus.CounterVec
}

// NewMetrics creates the bounded gatewayd metrics surface.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		ensureOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gatewayd_ensure_results_total",
			Help: "Total token ensure outcomes by configured provider and result state.",
		}, []string{"provider", "state"}),
	}
	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		metrics.ensureOutcomes,
	)
	for _, provider := range orderedProviders() {
		for _, state := range []string{string(EnsureStateFresh), string(EnsureStateIssued), ensureMetricStateFailed} {
			metrics.ensureOutcomes.WithLabelValues(string(provider), state)
		}
	}
	return metrics
}

func (metrics *Metrics) handler() http.Handler {
	if metrics == nil || metrics.registry == nil {
		metrics = NewMetrics()
	}
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}

func (metrics *Metrics) recordEnsure(provider TokenProvider, state EnsureState, failed bool) {
	if metrics == nil || metrics.ensureOutcomes == nil || !knownProvider(provider) {
		return
	}
	label := ensureMetricStateFailed
	if !failed {
		switch state {
		case EnsureStateFresh, EnsureStateIssued:
			label = string(state)
		}
	}
	metrics.ensureOutcomes.WithLabelValues(string(provider), label).Inc()
}
