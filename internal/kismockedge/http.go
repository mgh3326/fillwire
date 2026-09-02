package kismockedge

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
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

func writeCancelReceipt(writer http.ResponseWriter, status int, receipt CancelReceipt) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(receipt)
}
