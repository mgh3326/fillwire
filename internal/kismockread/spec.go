package kismockread

import "sort"

const (
	// MockBaseURL is the only KIS REST authority this binary can contact.
	MockBaseURL = "https://openapivts.koreainvestment.com:29443"
	MockHost    = "openapivts.koreainvestment.com:29443"
)

// Operation is a closed CLI operation vocabulary.
type Operation string

const (
	OperationUnknown              Operation = "unknown"
	OperationDomesticBalance      Operation = "domestic-balance"
	OperationOverseasBalance      Operation = "overseas-balance"
	OperationDomesticOrderHistory Operation = "domestic-order-history"
	OperationOverseasOrderHistory Operation = "overseas-order-history"
)

// ReadSpec describes one verified mock read endpoint. Every entry is GET-only
// and has an R-suffixed TR ID. No mutation TR belongs in this list.
type ReadSpec struct {
	Operation              Operation
	Path                   string
	TRID                   string
	RequestCursorFK        string
	RequestCursorNK        string
	ResponseCursorFK       string
	ResponseCursorNK       string
	ContinuationUsesHeader bool
	MaximumPages           int
}

var readSpecs = map[Operation]ReadSpec{
	OperationDomesticBalance: {
		Operation:              OperationDomesticBalance,
		Path:                   "/uapi/domestic-stock/v1/trading/inquire-balance",
		TRID:                   "VTTC8434R",
		RequestCursorFK:        "CTX_AREA_FK100",
		RequestCursorNK:        "CTX_AREA_NK100",
		ResponseCursorFK:       "CTX_AREA_FK100",
		ResponseCursorNK:       "CTX_AREA_NK100",
		ContinuationUsesHeader: true,
		MaximumPages:           10,
	},
	OperationOverseasBalance: {
		Operation:        OperationOverseasBalance,
		Path:             "/uapi/overseas-stock/v1/trading/inquire-balance",
		TRID:             "VTTS3012R",
		RequestCursorFK:  "CTX_AREA_FK200",
		RequestCursorNK:  "CTX_AREA_NK200",
		ResponseCursorFK: "CTX_AREA_FK200",
		ResponseCursorNK: "CTX_AREA_NK200",
		MaximumPages:     10,
	},
	OperationDomesticOrderHistory: {
		Operation:        OperationDomesticOrderHistory,
		Path:             "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
		TRID:             "VTTC8001R",
		RequestCursorFK:  "CTX_AREA_FK100",
		RequestCursorNK:  "CTX_AREA_NK100",
		ResponseCursorFK: "ctx_area_fk100",
		ResponseCursorNK: "ctx_area_nk100",
		MaximumPages:     100,
	},
	OperationOverseasOrderHistory: {
		Operation:        OperationOverseasOrderHistory,
		Path:             "/uapi/overseas-stock/v1/trading/inquire-ccnl",
		TRID:             "VTTS3035R",
		RequestCursorFK:  "CTX_AREA_FK200",
		RequestCursorNK:  "CTX_AREA_NK200",
		ResponseCursorFK: "ctx_area_fk200",
		ResponseCursorNK: "ctx_area_nk200",
		MaximumPages:     100,
	},
}

// ParseOperation canonicalizes the two short CLI aliases without widening the
// allowlist.
func ParseOperation(value string) (Operation, *SafeError) {
	switch value {
	case string(OperationDomesticBalance), "balance":
		return OperationDomesticBalance, nil
	case string(OperationOverseasBalance):
		return OperationOverseasBalance, nil
	case string(OperationDomesticOrderHistory), "orders":
		return OperationDomesticOrderHistory, nil
	case string(OperationOverseasOrderHistory), "overseas-orders":
		return OperationOverseasOrderHistory, nil
	default:
		return OperationUnknown, safeError(CodeInvalidInput)
	}
}

// LookupReadSpec returns a copy of a single allowlisted read specification.
func LookupReadSpec(operation Operation) (ReadSpec, bool) {
	spec, ok := readSpecs[operation]
	return spec, ok
}

// AllowedReadSpecs exposes copies for documentation and tests only.
func AllowedReadSpecs() []ReadSpec {
	operations := make([]string, 0, len(readSpecs))
	for operation := range readSpecs {
		operations = append(operations, string(operation))
	}
	sort.Strings(operations)

	specs := make([]ReadSpec, 0, len(operations))
	for _, operation := range operations {
		specs = append(specs, readSpecs[Operation(operation)])
	}
	return specs
}
