package kismockedge

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
)

const commandBodyLimit = 16 * 1024

// NewHandler exposes the approved local command, cancellation, and metrics
// boundaries. Authentication is intentionally not implied by this handler;
// the command listener binds only to loopback until a separately approved
// authentication design exists.
func NewHandler(service *Service) http.Handler {
	metrics := service.installMetrics()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.handler())
	mux.HandleFunc("POST /v1/commands", func(writer http.ResponseWriter, request *http.Request) {
		command, valid := decodeCommand(writer, request)
		if !valid {
			writeReceipt(writer, http.StatusBadRequest, receiptForInvalidHTTP(service))
			return
		}
		if command.AccountScope == executioncontracts.AccountScopeKISLive {
			receipt, code, err := service.ProcessWitness(request.Context(), command)
			if err != nil {
				writeShadowError(writer, http.StatusInternalServerError, ErrorStorageFailure)
				return
			}
			if code != "" {
				status := http.StatusBadRequest
				if code == ErrorScopeDisabled {
					status = http.StatusForbidden
				}
				metrics.recordLiveCommand(code)
				writeShadowError(writer, status, code)
				return
			}
			metrics.recordLiveCommand("recorded")
			service.refreshMissingWitnessMetric(request.Context())
			writeWitnessReceipt(writer, http.StatusOK, receipt)
			return
		}
		receipt, err := service.Process(request.Context(), command)
		if err != nil {
			// Do not expose database or transport details. A persistence failure
			// cannot safely be represented as a reusable NOT_CREATED receipt.
			receipt = service.receipt(command.CommandID, executioncontracts.DispositionUnknown, ErrorStorageFailure)
			writeReceipt(writer, http.StatusInternalServerError, receipt)
			return
		}
		writeReceipt(writer, http.StatusOK, receipt)
	})
	mux.HandleFunc("GET /v1/commands", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if query.Get("scope") != executioncontracts.AccountScopeKISLive || query.Get("missing_echo") != "true" || len(query) != 2 {
			writeShadowError(writer, http.StatusBadRequest, ErrorInvalidCommand)
			return
		}
		witnesses, err := service.MissingWitnesses(request.Context())
		if err != nil {
			writeShadowError(writer, http.StatusInternalServerError, ErrorStorageFailure)
			return
		}
		metrics.setMissingWitnesses(len(witnesses))
		writer.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(writer).Encode(struct {
			Witnesses []WitnessReceipt `json:"witnesses"`
		}{Witnesses: witnesses})
	})
	mux.HandleFunc("POST /v1/commands/{command_id}/echo", func(writer http.ResponseWriter, request *http.Request) {
		echo, valid := decodeWitnessEcho(writer, request)
		if !valid {
			writeShadowError(writer, http.StatusBadRequest, ErrorInvalidCommand)
			return
		}
		receipt, code, err := service.AttachWitnessEcho(request.Context(), request.PathValue("command_id"), echo)
		if err != nil {
			writeShadowError(writer, http.StatusInternalServerError, ErrorStorageFailure)
			return
		}
		if code != "" {
			status := http.StatusBadRequest
			switch code {
			case "witness_not_found":
				status = http.StatusNotFound
			case "echo_already_recorded":
				status = http.StatusConflict
			}
			writeShadowError(writer, status, code)
			return
		}
		service.refreshMissingWitnessMetric(request.Context())
		writeWitnessReceipt(writer, http.StatusOK, receipt)
	})
	mux.HandleFunc("GET /v1/price-band", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if len(query) != 2 {
			writeShadowError(writer, http.StatusBadRequest, ErrorInvalidCommand)
			return
		}
		band, code := service.PriceBand(request.Context(), query.Get("scope"), query.Get("stock_code"))
		if code != "" {
			status := http.StatusBadGateway
			if code == ErrorInvalidCommand {
				status = http.StatusBadRequest
			}
			writeShadowError(writer, status, code)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(writer).Encode(band)
	})
	mux.HandleFunc("POST /v1/commands/{command_id}/resolve", func(writer http.ResponseWriter, request *http.Request) {
		check, code := service.ResolveCommand(request.Context(), request.PathValue("command_id"))
		if code != "" {
			status := http.StatusBadGateway
			switch code {
			case ErrorInvalidCommand:
				status = http.StatusBadRequest
			case ErrorCommandNotFound:
				status = http.StatusNotFound
			case ErrorStorageFailure:
				status = http.StatusInternalServerError
			}
			writeShadowError(writer, status, code)
			return
		}
		writer.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(writer).Encode(check)
	})
	// GET /v1/cancel-carryover lists canary commands on one KIS mock scope whose
	// cancel is not confirmed, placed inside one trading-day window. Read-only.
	mux.HandleFunc("GET /v1/cancel-carryover", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		from, fromErr := time.Parse(time.RFC3339, query.Get("placed_from"))
		before, beforeErr := time.Parse(time.RFC3339, query.Get("placed_before"))
		scope := query.Get("scope")
		prefix := query.Get("command_prefix")
		if len(query) != 4 || (scope != executioncontracts.AccountScopeKISMock && scope != executioncontracts.AccountScopeKISMockUS) ||
			fromErr != nil || beforeErr != nil || !before.After(from) || !validCancelPrefix(prefix) {
			writeShadowError(writer, http.StatusBadRequest, ErrorInvalidCommand)
			return
		}
		candidates, err := service.CancelCarryOverCandidates(request.Context(), scope, prefix, from, before)
		if err != nil {
			writeShadowError(writer, http.StatusInternalServerError, ErrorStorageFailure)
			return
		}
		type candidateJSON struct {
			CommandID    string `json:"command_id"`
			AccountScope string `json:"account_scope"`
			PlacedAt     string `json:"placed_at"`
			Attempts     int    `json:"attempts"`
			CancelState  string `json:"cancel_state"`
			Actionable   bool   `json:"actionable"`
			Disposition  string `json:"disposition"`
		}
		out := make([]candidateJSON, 0, len(candidates))
		for _, candidate := range candidates {
			out = append(out, candidateJSON{CommandID: candidate.CommandID, AccountScope: candidate.AccountScope, PlacedAt: candidate.PlacedAt, Attempts: candidate.Attempts, CancelState: candidate.LatestState, Actionable: candidate.Actionable(), Disposition: candidate.Disposition})
		}
		writer.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(writer).Encode(struct {
			Candidates []candidateJSON `json:"candidates"`
		}{Candidates: out})
	})
	mux.HandleFunc("POST /v1/commands/{command_id}/cancel", func(writer http.ResponseWriter, request *http.Request) {
		receipt, err := service.Cancel(request.Context(), request.PathValue("command_id"))
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, errCancelNotEligible) {
				status = http.StatusConflict
			}
			writeCancelReceipt(writer, status, receipt)
			return
		}
		writeCancelReceipt(writer, http.StatusOK, receipt)
	})
	return metrics.instrument(mux)
}

func decodeWitnessEcho(writer http.ResponseWriter, request *http.Request) (WitnessEcho, bool) {
	defer request.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, commandBodyLimit))
	decoder.DisallowUnknownFields()
	var echo WitnessEcho
	if err := decoder.Decode(&echo); err != nil {
		return WitnessEcho{}, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return WitnessEcho{}, false
	}
	return echo, true
}

func decodeCommand(writer http.ResponseWriter, request *http.Request) (executioncontracts.ExecutionCommandV1, bool) {
	defer request.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, commandBodyLimit))
	decoder.DisallowUnknownFields()
	var command executioncontracts.ExecutionCommandV1
	if err := decoder.Decode(&command); err != nil {
		return executioncontracts.ExecutionCommandV1{}, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return executioncontracts.ExecutionCommandV1{}, false
	}
	return command, true
}

func receiptForInvalidHTTP(service *Service) executioncontracts.ExecutionReceiptV1 {
	if service == nil {
		return (&Service{}).receipt("", executioncontracts.DispositionNotCreated, ErrorInvalidCommand)
	}
	return service.receipt("", executioncontracts.DispositionNotCreated, ErrorInvalidCommand)
}

func writeReceipt(writer http.ResponseWriter, status int, receipt executioncontracts.ExecutionReceiptV1) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(receipt)
}

func writeWitnessReceipt(writer http.ResponseWriter, status int, receipt WitnessReceipt) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(receipt)
}

func writeShadowError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		ErrorCode string `json:"error_code"`
	}{ErrorCode: strings.TrimSpace(code)})
}

// validCancelPrefix accepts a short printable-ASCII prefix. The listing is
// scoped by prefix, so an empty prefix would list every mock cancel marker.
func validCancelPrefix(prefix string) bool {
	if prefix == "" || len(prefix) > 64 {
		return false
	}
	for _, character := range prefix {
		if character <= ' ' || character > '~' {
			return false
		}
	}
	return true
}

func writeCancelReceipt(writer http.ResponseWriter, status int, receipt CancelReceipt) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(receipt)
}
