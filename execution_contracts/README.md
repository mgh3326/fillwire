# execution_contracts

`ExecutionCommandV1`, `ExecutionReceiptV1`, and `TokenLeaseView` are small named
JSON shapes. They are not a ledger or token transport.

`ExecutionCommandV1` is the paper-only input for the separately approved local
`kis-mock-edge` boundary. `quantity` and `price` are strings, intentionally
preserved exactly through validation and the broker request. Its closed account
scope vocabulary is `kis_mock` (KIS VTS) and `alpaca_paper_crypto` (Alpaca
paper crypto only). For the latter scope, the existing `stock_code` wire field
carries the allowlisted Alpaca symbol `BTC/USD`; the wire shape is unchanged.

`ExecutionReceiptV1.disposition` is closed to `NOT_CREATED`, `ACCEPTED`, and
`UNKNOWN`. `UNKNOWN` is intentional after the broker HTTP boundary: it prevents
a retry from asserting that a potentially sent order was never created.

Python remains the owner of `order_send_intents`; it is not redirected to this
Go service by these contracts.

## KIS live shadow witness v1 (2026-09-02)

`kis_live` reuses the command field shape solely to preserve Python's intent.
It is accepted only by the Phase 1 shadow witness and never becomes a broker
request. The response and echo wire schema are in
[`kis_live_shadow_witness_v1.schema.json`](kis_live_shadow_witness_v1.schema.json).
Python continues to own approvals, idempotency reservation, live submission,
accepted-only ledger entries, reconciliation, and loss-sale protection.
