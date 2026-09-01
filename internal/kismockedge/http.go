package kismockedge

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

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

func writeCancelReceipt(writer http.ResponseWriter, status int, receipt CancelReceipt) {
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(receipt)
}
