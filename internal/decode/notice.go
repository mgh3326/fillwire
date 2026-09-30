package decode

import (
	"strings"

	"github.com/mgh3326/go-kis/kis/ws"
)

// Field indices of a decrypted KIS domestic execution notice (H0STCNI0 live,
// H0STCNI9 mock; one layout). The KIS column order is CUST_ID, ACNT_NO,
// ODER_NO, OODER_NO, SELN_BYOV_CLS, RCTF_CLS, ODER_KIND, ODER_COND,
// STCK_SHRN_ISCD, CNTG_QTY, CNTG_UNPR, STCK_CNTG_HOUR, RFUS_YN, CNTG_YN,
// ACPT_YN, BRNC_NO, ODER_QTY, ACNT_NAME, ORD_COND_PRC, ORD_EXG_GB, POPUP_YN,
// FILLER, CRDT_CLS, CRDT_LOAN_DATE, CNTG_ISNM40, ODER_PRC. The indices shared
// with go-kis (kis/ws/execution.go idxOrderNo..idxFilled) are restated here so
// a drift in either table is caught by checkExecutionMatchesFrame instead of
// silently shifting a fill field.
const (
	idxOrderNo  = 2  // ODER_NO
	idxSide     = 4  // SELN_BYOV_CLS
	idxRctfCls  = 5  // RCTF_CLS: 0 normal, 1 modify, 2 cancel
	idxSymbol   = 8  // STCK_SHRN_ISCD
	idxCntgQty  = 9  // CNTG_QTY
	idxCntgUnpr = 10 // CNTG_UNPR
	idxCntgHour = 11 // STCK_CNTG_HOUR
	idxRfusYN   = 12 // RFUS_YN: 0 accepted, 1 refused
	idxCntgYN   = 13 // CNTG_YN: 1 order/modify/cancel/refuse notice, 2 fill
	idxAcptYN   = 14 // ACPT_YN: 1 order accepted, 2 confirmed, 3 canceled (FOK/IOC)

	// minNoticeFields is the shortest frame that carries every field the
	// classification reads. A shorter frame cannot be proven to be a fill.
	minNoticeFields = idxAcptYN + 1
)

// NoticeKind is the classification of one KIS execution-notice frame. Only
// NoticeFill may become an execution-ledger record.
type NoticeKind string

const (
	// NoticeFill is an execution: CNTG_YN=2 with no refuse or cancel marker.
	NoticeFill NoticeKind = "fill"
	// NoticeOrder is an order accept, modify or cancel confirmation (CNTG_YN=1).
	NoticeOrder NoticeKind = "order_notice"
	// NoticeRejected is a broker refusal (RFUS_YN=1).
	NoticeRejected NoticeKind = "rejected"
	// NoticeCanceled is a cancel notice (RCTF_CLS=2 or ACPT_YN=3).
	NoticeCanceled NoticeKind = "canceled"
	// NoticeUnknown covers every frame this package cannot prove to be one of
	// the above: an unexpected TR, a short frame, or an unrecognized code.
	NoticeUnknown NoticeKind = "unknown"
)

// ClassifyNotice decides from the decrypted frame fields alone whether a KIS
// domestic execution notice is a fill. It fails closed: any value outside the
// documented vocabulary is NoticeUnknown, never NoticeFill. The rule order
// matches auto_trader's classifier (refuse, then cancel, then CNTG_YN).
func ClassifyNotice(tr string, fields []string) NoticeKind {
	if !ws.IsExecution(tr) || len(fields) < minNoticeFields {
		return NoticeUnknown
	}
	rfus := codeField(fields, idxRfusYN)
	rctf := codeField(fields, idxRctfCls)
	acpt := codeField(fields, idxAcptYN)
	cntg := codeField(fields, idxCntgYN)

	switch rfus {
	case "0":
	case "1":
		return NoticeRejected
	default:
		return NoticeUnknown
	}
	switch rctf {
	case "0", "1":
	case "2":
		return NoticeCanceled
	default:
		return NoticeUnknown
	}
	switch acpt {
	case "1", "2":
	case "3":
		return NoticeCanceled
	default:
		return NoticeUnknown
	}
	switch cntg {
	case "2":
		return NoticeFill
	case "1":
		return NoticeOrder
	default:
		return NoticeUnknown
	}
}

// codeField returns a one-digit KIS code field with surrounding spaces and
// zero padding removed, so "0", "00" and " 2" compare by value. Anything that
// is not all digits is returned as-is and therefore matches no known code.
func codeField(fields []string, index int) string {
	value := strings.TrimSpace(fields[index])
	if value == "" || strings.Trim(value, "0123456789") != "" {
		return value
	}
	trimmed := strings.TrimLeft(value, "0")
	if trimmed == "" {
		return "0"
	}
	return trimmed
}

// checkExecutionMatchesFrame reports whether the go-kis parsed execution
// carries exactly the frame values at fillwire's own indices. A mismatch means
// one of the two index tables drifted, so no fill field can be trusted.
func checkExecutionMatchesFrame(execution *ws.Execution, fields []string) bool {
	if len(fields) < minNoticeFields {
		return false
	}
	return execution.OrderNo == fields[idxOrderNo] &&
		execution.Symbol == fields[idxSymbol] &&
		execution.Qty == fields[idxCntgQty] &&
		execution.Price == fields[idxCntgUnpr] &&
		execution.FilledAt == fields[idxCntgHour] &&
		execution.Filled == fields[idxCntgYN] &&
		execution.Side == frameSide(fields[idxSide])
}

func frameSide(code string) ws.Side {
	switch strings.TrimSpace(code) {
	case "01", "1", "S":
		return ws.SideSell
	case "02", "2", "B":
		return ws.SideBuy
	default:
		return ws.SideUnknown
	}
}
