package kismockread

import (
	"net/url"
	"reflect"
	"testing"
)

func TestOverseasOrderHistoryMirrorsVTSReferenceRequest(t *testing.T) {
	spec, found := LookupReadSpec(OperationOverseasOrderHistory)
	if !found {
		t.Fatal("missing overseas order-history read spec")
	}
	if spec.Path != "/uapi/overseas-stock/v1/trading/inquire-ccnl" || spec.TRID != "VTTS3035R" {
		t.Fatalf("overseas spec=%+v", spec)
	}
	input := ReadRequest{
		Operation: OperationOverseasOrderHistory,
		FromDate:  "20260901",
		ToDate:    "20260901",
		Side:      "00",
		Exchange:  "NASD",
	}
	got := readQuery(spec, input, "12345678", "01", "", "")
	want := url.Values{
		"ACNT_PRDT_CD":   {"01"},
		"CANO":           {"12345678"},
		"CCLD_NCCS_DVSN": {"00"},
		"CTX_AREA_FK200": {""},
		"CTX_AREA_NK200": {""},
		"ODNO":           {""},
		"ORD_DT":         {""},
		"ORD_END_DT":     {"20260901"},
		"ORD_GNO_BRNO":   {""},
		"ORD_STRT_DT":    {"20260901"},
		"OVRS_EXCG_CD":   {"NASD"},
		"PDNO":           {""},
		"SLL_BUY_DVSN":   {"00"},
		"SORT_SQN":       {"DS"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("overseas daily order query = %#v, want %#v", got, want)
	}
	requestURL, err := buildReadURL(MockBaseURL, spec, input, "12345678", "01", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if requestURL.Scheme != "https" || requestURL.Host != MockHost || requestURL.Path != spec.Path {
		t.Fatalf("VTS request URL = %s", requestURL)
	}
}
