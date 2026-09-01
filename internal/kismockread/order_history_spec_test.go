package kismockread

import (
	"net/url"
	"reflect"
	"testing"
)

// This is the exact VTS request shape used by auto_trader's
// DomesticOrders.inquire_daily_order_domestic implementation. Keep the
// regression test here so a parameter change cannot silently diverge from the
// only measured GET-only reconciliation path.
func TestDomesticOrderHistoryMirrorsVTSReferenceRequest(t *testing.T) {
	spec, found := LookupReadSpec(OperationDomesticOrderHistory)
	if !found {
		t.Fatal("missing domestic order-history read spec")
	}
	if spec.TRID != "VTTC8001R" {
		t.Fatalf("TR ID = %q, want VTTC8001R", spec.TRID)
	}

	input := ReadRequest{
		Operation: OperationDomesticOrderHistory,
		FromDate:  "20260901",
		ToDate:    "20260901",
		Side:      "00",
	}
	got := readQuery(spec, input, "12345678", "01", "", "")
	want := url.Values{
		"ACNT_PRDT_CD":    {"01"},
		"CANO":            {"12345678"},
		"CCLD_DVSN":       {"00"},
		"CTX_AREA_FK100":  {""},
		"CTX_AREA_NK100":  {""},
		"EXCG_ID_DVSN_CD": {"ALL"},
		"INQR_DVSN":       {"00"},
		"INQR_DVSN_1":     {""},
		"INQR_DVSN_3":     {"00"},
		"INQR_END_DT":     {"20260901"},
		"INQR_STRT_DT":    {"20260901"},
		"ODNO":            {""},
		"ORD_GNO_BRNO":    {""},
		"PDNO":            {""},
		"SLL_BUY_DVSN_CD": {"00"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("daily order query = %#v, want %#v", got, want)
	}

	requestURL, err := buildReadURL(MockBaseURL, spec, input, "12345678", "01", "", "")
	if err != nil {
		t.Fatalf("buildReadURL: %v", err)
	}
	if requestURL.Scheme != "https" || requestURL.Host != MockHost || requestURL.Path != spec.Path {
		t.Fatalf("VTS request URL = %s", requestURL)
	}
}
