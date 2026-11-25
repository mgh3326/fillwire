// Package marketminute is the shadow KIS REST minute-chart poller: it reads
// only the domestic (FHKST03010200) and overseas (HHDFS76950200) minute GET
// transactions, applies auto_trader's closed-bar and accumulated-value rules,
// and writes rows to JSONL and/or Redis Streams.
//
// The package imports no order, ledger or token-issuance code. The access
// token is supplied by the caller (Redis read or gatewayd fetch). The row
// bytes are pinned to Python goldens in testdata/bars, and the session labels
// to session_segment_of goldens in testdata/segments.golden.json.
//
// The domestic transaction is FHKST03010200, the value auto_trader's
// constants.DOMESTIC_MINUTE_CHART_TR names for inquire_minute_chart. The
// design note's FHKST03010230 spelling does not match that reader or the
// inquiries path; parity with the Python collector wins here.
package marketminute
