# kis-mock-edge boundary design

## Approved scope

`kis-mock-edge` is a deliberately narrow, local HTTP receiver for a closed set
of paper-only placement commands: Korean equity **mock VTS** and Alpaca paper
crypto. It is the separately approved evolution of the formerly reserved
command name; it does not change Python ownership of `order_send_intents`,
redirect Python traffic, or modify `auto_trader`.

The receiver exposes `POST /v1/commands` and
`POST /v1/commands/{command_id}/cancel`, and binds to `127.0.0.1:8080` by
default. `BROKER_EDGE_LISTEN_ADDR` may select another loopback address (for
example `127.0.0.1:0` in tests), but cannot expose the unauthenticated receiver
on a non-loopback interface. Authentication and authorization remain a later,
separate design.

## Command and receipt boundary

`execution_contracts.ExecutionCommandV1` has only these fields:
`schema_version`, `command_id`, `account_scope`, `side`, `stock_code`,
`quantity`, `price`, `order_type`, and `issued_at`. Account scope is closed to
`kis_mock` and `alpaca_paper_crypto`; both accept only `limit` orders so their
fixed notional caps can be proven before sending. The wire shape is unchanged:
for `alpaca_paper_crypto`, `stock_code` carries the Alpaca symbol.

Price and quantity are decimal strings and are sent unchanged. For `kis_mock`,
quantity and price must be positive integer strings; the KRX tick table and
buy-floor/sell-ceiling logic are ported as pure validation functions from
`auto_trader`, and a nonmatching limit price receives `NOT_CREATED` with
`tick_mismatch`. For `alpaca_paper_crypto`, both values must be positive strict
decimal strings and no KRX tick check is applied. The edge never adjusts or
re-prices a command.

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
HTTP operation may have reached a broker, a restart returns that `UNKNOWN`
receipt and never resends. Broker timeouts, 5xx responses, redirects, malformed
responses, and non-accepted responses after this point remain `UNKNOWN`; they
are never relabeled `NOT_CREATED`.

## Cancellation boundary

Cancellation is not a new order command: the path parameter must identify an
effective `ACCEPTED` receipt with a stored broker order id. Missing,
`NOT_CREATED`, and `UNKNOWN` commands are rejected before broker preparation
or network activity. The additive `cancel_attempts` table has a unique command
id and records one closed cancellation state. It commits `UNKNOWN` immediately
before the cancellation request; duplicate calls replay it and never send
again. A completed cancellation is `CANCELLED`, a provider 404 is
`NOT_FOUND/cancel_not_found`, and every ambiguous post-send outcome remains
`UNKNOWN`.

Alpaca paper cancellation is `DELETE /v2/orders/{broker_order_id}` and only a
204 result is `CANCELLED`. KIS mock cancellation uses the VTS-pinned
`VTTC0013U` `order-rvsecncl` TR with its domestic cancellation body, including
the stored order number as `ORGN_ODNO`, `RVSE_CNCL_DVSN_CD="02"`, and KRX
routing. It does not introduce a live KIS authority or credential path.

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

For `kis_mock`, immutable process constants cap a command at 100 shares and KRW
1,000,000 notional. There is no environment override that can raise either cap.
The only KIS broker URL is the pinned HTTPS VTS authority
`openapivts.koreainvestment.com:29443`; redirects are refused and the final URL
is rechecked inside the transport.

The edge reuses `kis-mock-read`'s strict cached-token loader. It can only issue
Redis `GET` and validate the existing Python-written token payload. It has no
token issuance, refresh, clear, migration, write, or lock capability.

## Alpaca paper crypto scope

`alpaca_paper_crypto` is a separate `Broker` implementation selected only when
`account_scope` exactly matches that value. It has no Redis or KIS-token
dependency. It reads only `ALPACA_PAPER_CRYPTO_API_KEY` and
`ALPACA_PAPER_CRYPTO_API_SECRET`; these static values are used only as the
`APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` request headers and are never
returned, logged, or placed in a receipt.

The scope allows only `buy` `BTC/USD` orders. `quantity` and `price` are
positive decimal strings; their exact product may not exceed USD 10. There is
no environment override for the allowlist, side, or cap. The request is one
`POST /v2/orders` body with `symbol`, `qty`, `side`, `type="limit"`,
`limit_price`, and `time_in_force="gtc"`. A 2xx response is accepted only when
it contains a nonempty string `id` and a `status` field.

The only permitted authority is `https://paper-api.alpaca.markets`. HTTP,
userinfo, fragments, ports, redirects, and every other host are refused. The
live `https://api.alpaca.markets` authority is a compile-time forbidden value in
the same spirit as `auto_trader`'s `FORBIDDEN_TRADING_BASE_URLS`; it cannot be
set through the environment. The pin is checked when constructing the request
and again inside the RoundTripper immediately before any transport call.

## Explicit exclusions

- No live KIS host, live credentials, or live place route exists here.
- No live Alpaca host, live credential name, or host override exists here.
- No modification, market order, or other mutation endpoint is exposed.
- `POST /v1/commands/{command_id}/cancel` may cancel only a durably
  `ACCEPTED` command with a broker order id. It persists one closed result:
  `CANCELLED`, `NOT_FOUND` (`cancel_not_found`), or `UNKNOWN`; duplicate POSTs
  replay that result and never resend the broker request.
- No retry or replay path can issue a second broker POST for a command ID.
- No account, order, or token values are emitted as metrics or response data.
- This repository's tests use fake transports only; they do not execute a real
  KIS VTS or Alpaca paper placement.
