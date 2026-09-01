# execution_contracts

`ExecutionCommandV1`, `ExecutionReceiptV1`, and `TokenLeaseView` are small named
JSON shapes. They are not a ledger or token transport.

`ExecutionCommandV1` is the mock-only input for the separately approved local
`kis-mock-edge` boundary. `quantity` and `price` are strings, intentionally
preserved exactly through validation and the broker request. Its fixed account
scope is `kis_mock`.

`ExecutionReceiptV1.disposition` is closed to `NOT_CREATED`, `ACCEPTED`, and
`UNKNOWN`. `UNKNOWN` is intentional after the broker HTTP boundary: it prevents
a retry from asserting that a potentially sent order was never created.

Python remains the owner of `order_send_intents`; it is not redirected to this
Go service by these contracts.
