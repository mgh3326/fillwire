# kis-mock-edge boundary design

## Approved scope

`kis-mock-edge` is a deliberately narrow, local HTTP receiver for one Korean
equity **mock VTS** placement command. It is the separately approved evolution
of the formerly reserved command name; it does not change Python ownership of
`order_send_intents`, redirect Python traffic, or modify `auto_trader`.

The receiver exposes only `POST /v1/commands` and binds to `127.0.0.1:8080` by
default. `BROKER_EDGE_LISTEN_ADDR` may select another loopback address (for
example `127.0.0.1:0` in tests), but cannot expose the unauthenticated receiver
on a non-loopback interface. Authentication and authorization remain a later,
separate design.

## Command and receipt boundary

`execution_contracts.ExecutionCommandV1` has only these fields:
`schema_version`, `command_id`, `account_scope`, `side`, `stock_code`,
`quantity`, `price`, `order_type`, and `issued_at`. The only account scope is
`kis_mock`; sides are `buy` or `sell`; this first edge accepts only `limit`
orders so its fixed notional cap can be proven before sending.

Price and quantity are decimal strings and are sent unchanged. Quantity must
be a positive integer string. The KRX tick table and buy-floor/sell-ceiling
logic are ported as pure validation functions from `auto_trader`; a nonmatching
limit price receives `NOT_CREATED` with `tick_mismatch`. The edge never adjusts
or re-prices a command.

`ExecutionReceiptV1` is the only successful command response shape. Its
`disposition` is a closed vocabulary:

- `NOT_CREATED`: a deterministic pre-send check failed.
- `ACCEPTED`: the mock broker returned `rt_cd="0"` and an order number.
- `UNKNOWN`: the pending send boundary was crossed but provider acceptance
  cannot be proved.

`broker_order_id` and `error_code` are optional. The response does not contain
broker payloads, credentials, account identifiers, order details, or tokens.

## Durable idempotency and failure semantics

The local SQLite `commands` table has a `command_id UNIQUE` constraint and
stores the receipt. Reusing a command ID always returns its durable receipt and
does not POST to the broker again.

Immediately before the single broker POST, the edge commits an `UNKNOWN`
pending receipt. If the process dies after that commit, including while the
HTTP operation may have reached KIS, a restart returns that `UNKNOWN` receipt
and never resends. Broker timeouts, 5xx responses, redirects, malformed
responses, and non-accepted responses after this point remain `UNKNOWN`; they
are never relabeled `NOT_CREATED`.

`kis-mock-edge resolve` is the only later conclusion path. It queries the
already allowlisted, GET-only VTS domestic daily-order history endpoint
(`VTTC8001R`) and appends a resolution record; it does not alter the original
receipt. A single order matching side, stock, quantity, price, and the pending
send timestamp within its fixed five-minute match window yields `ACCEPTED` and
the broker order number. A successful complete query with no corresponding
order may yield `NOT_CREATED/resolved_absent` only after a ten-minute grace
period. Query failure, ambiguous matches, and an unexpired grace period remain
`UNKNOWN` without a resolution record. Legacy receipt rows that predate stored
command facts are resolved absent only when the successful day query contains
zero orders.

## Mock-only placement gates

Placement is disabled unless `BROKER_EDGE_MOCK_PLACE_ENABLED=true` is explicit.
When disabled, no configuration load, Redis access, or broker transport call
is attempted and the receipt is `NOT_CREATED/place_disabled`.

Even when enabled, immutable process constants cap a command at 100 shares and
KRW 1,000,000 notional. There is no environment override that can raise either
cap. The only broker URL is the pinned HTTPS VTS authority
`openapivts.koreainvestment.com:29443`; redirects are refused and the final URL
is rechecked inside the transport.

The edge reuses `kis-mock-read`'s strict cached-token loader. It can only issue
Redis `GET` and validate the existing Python-written token payload. It has no
token issuance, refresh, clear, migration, write, or lock capability.

## Explicit exclusions

- No live KIS host, live credentials, or live place route exists here.
- No modification, cancellation, market order, or other mutation endpoint is
  exposed.
- No retry or replay path can issue a second broker POST for a command ID.
- No account, order, or token values are emitted as metrics or response data.
- This repository's tests use fake transports only; they do not execute a real
  VTS placement.
