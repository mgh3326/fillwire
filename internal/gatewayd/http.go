package gatewayd

import (
	"io"
	"net/http"
)

// EventLogger receives fixed event text only. The handler never passes an
// upstream error, token, credential, Redis URL, request body, or path value to
// it.
type EventLogger interface {
	Print(...any)
}

// ProviderEnsurers contains only explicitly configured providers. It is kept
// separate from provider parsing so an unconfigured live provider cannot be
// reached merely because a credential exists in the process environment.
type ProviderEnsurers map[TokenProvider]Ensurer

// NewHandler exposes loopback-safe health, Prometheus metrics, and the closed
// token ensure route. The provider segment is parsed as a closed enum before
// it is used.
func NewHandler(ensurers ProviderEnsurers, logger EventLogger) http.Handler {
	return newHandler(ensurers, logger, NewMetrics())
}

func newHandler(ensurers ProviderEnsurers, logger EventLogger, metrics *Metrics) http.Handler {
	if metrics == nil {
		metrics = NewMetrics()
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.handler())
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("POST /v1/tokens/{provider}/ensure", func(writer http.ResponseWriter, request *http.Request) {
		provider := TokenProvider(request.PathValue("provider"))
		if !knownProvider(provider) {
			logEvent(logger, "gatewayd: token ensure unavailable")
			writeJSON(writer, http.StatusNotFound, `{"error":"not_found"}`)
			return
		}
		ensurer, configured := ensurers[provider]
		if !configured || ensurer == nil {
			logEvent(logger, "gatewayd: token ensure unavailable")
			metrics.recordEnsure(provider, "", true)
			writeJSON(writer, http.StatusNotFound, `{"error":"not_found"}`)
			return
		}
		state, err := ensurer.Ensure(request.Context())
		if err != nil {
			logEvent(logger, "gatewayd: token ensure failed")
			metrics.recordEnsure(provider, "", true)
			writeJSON(writer, http.StatusServiceUnavailable, `{"error":"ensure_failed"}`)
			return
		}
		switch state {
		case EnsureStateFresh:
			logEvent(logger, "gatewayd: token ensure fresh")
			metrics.recordEnsure(provider, state, false)
			writeJSON(writer, http.StatusOK, `{"state":"fresh"}`)
		case EnsureStateIssued:
			logEvent(logger, "gatewayd: token ensure issued")
			metrics.recordEnsure(provider, state, false)
			writeJSON(writer, http.StatusOK, `{"state":"issued"}`)
		default:
			logEvent(logger, "gatewayd: token ensure unavailable")
			metrics.recordEnsure(provider, "", true)
			writeJSON(writer, http.StatusServiceUnavailable, `{"error":"ensure_failed"}`)
		}
	})
	return mux
}

func logEvent(logger EventLogger, message string) {
	if logger != nil {
		logger.Print(message)
	}
}

func writeJSON(writer http.ResponseWriter, status int, body string) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, body)
}
