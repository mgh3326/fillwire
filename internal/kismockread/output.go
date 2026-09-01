package kismockread

import (
	"encoding/json"
	"fmt"
	"io"
)

const outputSchemaVersion = "kis-mock-read/v1"

// Output is the closed JSON schema emitted by --json. It contains summaries
// only, never raw broker JSON, request headers, credentials, or token values.
type Output struct {
	SchemaVersion string    `json:"schema_version"`
	Operation     Operation `json:"operation"`
	Status        string    `json:"status"`
	ErrorCode     ErrorCode `json:"error_code"`
	TRID          string    `json:"tr_id"`
	Pages         int       `json:"pages"`
	Records       int       `json:"records"`
}

func successOutput(result Result) Output {
	return Output{
		SchemaVersion: outputSchemaVersion,
		Operation:     result.Operation,
		Status:        "ok",
		ErrorCode:     "",
		TRID:          result.TRID,
		Pages:         result.Pages,
		Records:       result.Records,
	}
}

func failureOutput(operation Operation, code ErrorCode) Output {
	trID := ""
	if spec, found := LookupReadSpec(operation); found {
		trID = spec.TRID
	}
	return Output{
		SchemaVersion: outputSchemaVersion,
		Operation:     operation,
		Status:        "error",
		ErrorCode:     code,
		TRID:          trID,
		Pages:         0,
		Records:       0,
	}
}

func writeJSONOutput(writer io.Writer, output Output) {
	_ = json.NewEncoder(writer).Encode(output)
}

func writeHumanSuccess(writer io.Writer, result Result) {
	_, _ = fmt.Fprintf(
		writer,
		"OK %s: pages=%d records=%d tr_id=%s\n",
		result.Operation,
		result.Pages,
		result.Records,
		result.TRID,
	)
}

func writeHumanFailure(writer io.Writer, code ErrorCode) {
	_, _ = fmt.Fprintf(writer, "ERROR: %s\n", code)
}
