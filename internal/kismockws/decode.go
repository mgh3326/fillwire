// Package kismockws records KIS VTS quotes and fill notices. It has no order
// or ledger-ingest capability.
package kismockws

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/go-kis/kis/ws"
)

const (
	TROverseasNotice      = "H0GSCNI9"
	TRFuturesNotice       = "H0IFCNI0"
	TRFuturesNoticeLegacy = "H0IFCNI9"
)

// Notice is one decrypted KIS account-notice row, named in the order of the
// official open-trading-api examples_user websocket samples.
type Notice struct {
	Kind       string            `json:"kind"`
	TR         string            `json:"tr"`
	ReceivedAt time.Time         `json:"received_at"`
	Fields     map[string]string `json:"fields"`
	Raw        []byte            `json:"raw,omitempty"`
}

var overseasNoticeFields = []string{
	"CUST_ID", "ACNT_NO", "ODER_NO", "OODER_NO", "SELN_BYOV_CLS", "RCTF_CLS",
	"ODER_KIND2", "STCK_SHRN_ISCD", "CNTG_QTY", "CNTG_UNPR", "STCK_CNTG_HOUR",
	"RFUS_YN", "CNTG_YN", "ACPT_YN", "BRNC_NO", "ODER_QTY", "ACNT_NAME",
	"CNTG_ISNM", "ODER_COND", "DEBT_GB", "DEBT_DATE", "START_TM", "END_TM",
	"TM_DIV_TP", "CNTG_UNPR12",
}

var futuresNoticeFields = []string{
	"cust_id", "acnt_no", "oder_no", "ooder_no", "seln_byov_cls", "rctf_cls",
	"oder_kind2", "stck_shrn_iscd", "cntg_qty", "cntg_unpr", "stck_cntg_hour",
	"rfus_yn", "cntg_yn", "acpt_yn", "brnc_no", "oder_qty", "acnt_name",
	"cntg_isnm", "oder_cond", "ord_grp", "ord_grpseq", "order_prc",
}

func isFuturesNoticeTR(tr string) bool {
	return tr == TRFuturesNotice || tr == TRFuturesNoticeLegacy
}

// DecodeNotice maps the decrypted websocket fields to KIS's sample schema.
// The count check fails closed so a changed wire layout is counted and dropped.
func DecodeNotices(event ws.Event) ([]Notice, error) {
	var names []string
	switch event.TR {
	case TROverseasNotice:
		names = overseasNoticeFields
	case TRFuturesNotice, TRFuturesNoticeLegacy:
		names = futuresNoticeFields
	default:
		return nil, errors.New("kismockws: unsupported notice TR")
	}
	parts := strings.SplitN(string(event.Raw), "|", 4)
	if len(parts) != 4 || parts[1] != event.TR {
		return nil, errors.New("kismockws: malformed notice header")
	}
	count, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil || count < 1 || len(event.Fields) != count*len(names) {
		return nil, errors.New("kismockws: malformed notice field count")
	}
	at := event.ReceivedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	out := make([]Notice, 0, count)
	for row := range count {
		fields := make(map[string]string, len(names))
		for i, name := range names {
			fields[name] = event.Fields[row*len(names)+i]
		}
		out = append(out, Notice{Kind: classifyNotice(fields), TR: event.TR, ReceivedAt: at.UTC(), Fields: fields, Raw: event.Raw})
	}
	return out, nil
}

func classifyNotice(fields map[string]string) string {
	code := func(name string) string {
		value := strings.TrimSpace(fields[name])
		value = strings.TrimLeft(value, "0")
		if value == "" {
			return "0"
		}
		return value
	}
	switch {
	case code("rfus_yn") == "1" || code("RFUS_YN") == "1":
		return "rejected"
	case code("rctf_cls") == "2" || code("RCTF_CLS") == "2" || code("acpt_yn") == "3" || code("ACPT_YN") == "3":
		return "canceled"
	case code("cntg_yn") == "2" || code("CNTG_YN") == "2":
		return "fill"
	case code("cntg_yn") == "1" || code("CNTG_YN") == "1":
		return "order_notice"
	default:
		return "unknown"
	}
}

// DecodeQuote validates and parses a raw domestic quote frame with the shared
// book decoder. The exact wire payload is retained for later replay.
func DecodeQuote(event ws.Event) (any, error) {
	frame, err := book.ParseFrame(event.Raw)
	if err != nil {
		return nil, err
	}
	at := event.ReceivedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return struct {
		Kind       string     `json:"kind"`
		TR         string     `json:"tr"`
		ReceivedAt time.Time  `json:"received_at"`
		Frame      book.Frame `json:"frame"`
		Raw        []byte     `json:"raw"`
	}{"quote", event.TR, at.UTC(), frame, event.Raw}, nil
}
