package gatewayd

import (
	"io"
	"net/http"
)

// EventLogger receives fixed event text only. The handler never passes an
// upstream error, token, credential, Redis URL, or request body to it.
type EventLogger interface {
	Print(...any)
}

// NewHandler exposes only the local health and KIS mock token ensure routes.
func NewHandler(ensurer Ensurer, logger EventLogger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("POST /v1/tokens/kis-mock/ensure", func(writer http.ResponseWriter, request *http.Request) {
		if ensurer == nil {
			logEvent(logger, "gatewayd: token ensure unavailable")
			writeJSON(writer, http.StatusServiceUnavailable, `{"error":"ensure_failed"}`)
			return
		}
		state, err := ensurer.Ensure(request.Context())
		if err != nil {
			logEvent(logger, "gatewayd: token ensure failed")
			writeJSON(writer, http.StatusServiceUnavailable, `{"error":"ensure_failed"}`)
			return
		}
		switch state {
		case EnsureStateFresh:
			logEvent(logger, "gatewayd: token ensure fresh")
			writeJSON(writer, http.StatusOK, `{"state":"fresh"}`)
		case EnsureStateIssued:
			logEvent(logger, "gatewayd: token ensure issued")
			writeJSON(writer, http.StatusOK, `{"state":"issued"}`)
		default:
			logEvent(logger, "gatewayd: token ensure unavailable")
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
