# broker-edge

`broker-edge` contains deliberately bounded paper-trading paths:
`kis-mock-read` is KIS VTS GET-only, and `kis-mock-edge` is a separately
approved, loopback-only placement edge that routes a closed account scope to
either KIS VTS or Alpaca paper crypto. `cmd/` follows Go convention only—Python
does not import it, and Python retains ownership of `order_send_intents`.

## Read path: `kis-mock-read`

`kis-mock-read` reads an already-issued VTS token from Redis and makes a small,
GET-only allowlist of KIS mock account reads. It is fail-closed:

- Redis key: `kis_mock:{host}:{fp16}:access_token`, where `host` is the
  lowercased base-URL netloc and `fp16` is the first 16 hex characters of
  `sha256(KIS_MOCK_APP_KEY)`.
- Cached value must be exactly a JSON object with `access_token` and numeric
  `expires_at`; it is usable only when `now < expires_at - 60 seconds`.
- Missing, malformed, or near-expiry tokens stop the command. There is no token
  issuance, cache write, cache clear, refresh, or lock fallback.
- The REST authority is pinned to
  `https://openapivts.koreainvestment.com:29443`; HTTP, another host, and every
  redirect are rejected. The request URL is checked again immediately before
  transport.

The read allowlist was copied read-only from the specified `auto_trader` KIS
sources. Every operation is HTTP GET and its TR ID ends in `R`.

| CLI operation | Path | VTS TR ID |
| --- | --- | --- |
| `domestic-balance` (or `balance`) | `/uapi/domestic-stock/v1/trading/inquire-balance` | `VTTC8434R` |
| `overseas-balance` | `/uapi/overseas-stock/v1/trading/inquire-balance` | `VTTS3012R` |
| `domestic-order-history` (or `orders`) | `/uapi/domestic-stock/v1/trading/inquire-daily-ccld` | `VTTC8001R` |

The mock pending-order inquiry is deliberately excluded because the reference
implementation rejects it in mock mode. Integrated-margin, buyable-amount, and
other VTS behavior have no measurement in this bootstrap and remain UNKNOWN.

Human output is a count-only summary. `--json` emits this closed schema only:
`schema_version`, `operation`, `status`, `error_code`, `tr_id`, `pages`, and
`records`. It never emits a raw broker response, credential, Redis URL, or token
value.

## Read configuration and use

Only these environment variable names are read or documented:

- `KIS_MOCK_APP_KEY`
- `KIS_MOCK_APP_SECRET`
- `KIS_MOCK_ACCOUNT_NO`
- `REDIS_URL`

`KIS_MOCK_ACCOUNT_NO` accepts either an eight-digit CANO (which uses product
code `01`) or an explicit ten-digit / `12345678-01` account form.

[.env.example](.env.example) has placeholders only. Keep populated env files
out of this public repository.

```sh
go run ./cmd/kis-mock-read domestic-balance
go run ./cmd/kis-mock-read domestic-balance --json
go run ./cmd/kis-mock-read domestic-order-history --from 20260901 --to 20260901 --json
```

## Provider-scoped token gateway: `gatewayd`

`gatewayd` is a separate, loopback-only OAuth token issuer. It exposes
`GET /healthz` and `POST /v1/tokens/{provider}/ensure`, where `provider` is the
closed set `kis-mock`, `kis-live`, or `toss`. The ensure response is only
`{"state":"fresh"}` or `{"state":"issued"}`; it never includes a token.

The daemon defaults to `--providers=kis-mock`. A deployment must explicitly
select `kis-live` and/or `toss`; merely placing their credentials in the
environment cannot activate their issuance paths. The optional autonomous mode
also refreshes only that selected provider set:

```sh
go run ./cmd/gatewayd --ensure-interval=5m
go run ./cmd/gatewayd --providers=kis-mock,toss --ensure-interval=5m
```

All providers use Redis `SET NX EX 30`, recheck the cache under the lock, and
return a fixed error rather than bypass a contended lock. Their authority and
cache contracts are separately pinned:

- `kis-mock` uses the existing VTS authority and
  `kis_mock:{host}:{fp16}:access_token` three-field KIS JSON payload.
- `kis-live` posts only to `https://openapi.koreainvestment.com:9443/oauth2/tokenP`,
  uses `KIS_APP_KEY`/`KIS_APP_SECRET`, and mirrors the default Python namespace
  exactly: `kis:access_token` and `kis:token:lock`.
- `toss` posts only form-encoded client credentials to
  `https://openapi.tossinvest.com/oauth2/token`, uses
  `TOSS_API_CLIENT_ID`/`TOSS_API_CLIENT_SECRET`, and mirrors its Python cache
  key `toss:oauth:{sha256(client_id)[:16]}:access_token` plus its **two-field**
  JSON payload. Toss permits one valid token per client, so a replacement is
  written to Redis before the shared lock is released.

`gatewayd` has no order, command, intent, or execution endpoint.

## Mock placement edge: `kis-mock-edge`

`kis-mock-edge` accepts `POST /v1/commands` and
`POST /v1/commands/{command_id}/cancel` on `127.0.0.1:8080` by default. The
listener rejects non-loopback overrides. `account_scope` is closed
to `kis_mock` and `alpaca_paper_crypto`; both use the same SQLite receipt store,
where `command_id UNIQUE` means a repeated ID returns the saved receipt and
makes zero additional broker POSTs.

Placement is disabled by default. It is allowed only when
`BROKER_EDGE_MOCK_PLACE_ENABLED=true` is explicit. A pending `UNKNOWN` receipt
is committed immediately before the one possible broker POST. Therefore a kill,
timeout, 5xx, redirect, or ambiguous provider response cannot be retried as
`NOT_CREATED`.

The `kis_mock` path supports Korean limit orders only. It preserves price and
quantity strings verbatim, rejects invalid KRX ticks rather than adjusting them,
and has non-configurable caps of 100 shares and KRW 1,000,000 notional. It
reuses the read path's GET-only cached token loader; it cannot issue or mutate a
token.

The `alpaca_paper_crypto` smoke path is independent of Redis and KIS tokens.
It accepts only a `buy` limit order for `BTC/USD` (carried in the existing
`stock_code` field), with positive decimal `quantity` and `price` strings and a
non-configurable USD 10 notional cap. It sends those strings unchanged to only
`https://paper-api.alpaca.markets/v2/orders`, authenticating with static paper
key headers. The live `https://api.alpaca.markets` authority, HTTP, host
overrides, and redirects are all refused before a second transfer can occur.

Local settings:

- `BROKER_EDGE_MOCK_PLACE_ENABLED` — must be exactly `true` to permit a place.
- `BROKER_EDGE_LISTEN_ADDR` — optional loopback address; default
  `127.0.0.1:8080`.
- `BROKER_EDGE_SQLITE_PATH` — optional local receipt database; default
  `kis-mock-edge.sqlite`.
- `ALPACA_PAPER_CRYPTO_API_KEY` — static paper API key, used only for
  `alpaca_paper_crypto`.
- `ALPACA_PAPER_CRYPTO_API_SECRET` — static paper API secret, used only for
  `alpaca_paper_crypto`.

Run it only after the explicit gate is intentionally armed:

```sh
BROKER_EDGE_MOCK_PLACE_ENABLED=true go run ./cmd/kis-mock-edge
```

### Resolving an UNKNOWN receipt

`kis-mock-edge resolve` is a bounded, read-only reconciliation command. It
uses the existing GET-only domestic order-history route (`VTTC8001R`) for the
KIS mock account and never performs a placement, token issue, refresh, or
cache write.

```sh
go run ./cmd/kis-mock-edge resolve
go run ./cmd/kis-mock-edge resolve --grace=10m
```

For a stored `UNKNOWN/kis_mock` receipt, one matching order (side, stock,
quantity, price, and send-time window) appends an `ACCEPTED` resolution with
the broker order number. A successful complete read with no match appends
`NOT_CREATED/resolved_absent` only after the ten-minute default grace period.
Read errors, multiple matches, and young receipts remain `UNKNOWN`. The
original receipt row is preserved; resolution is an additive SQLite record and
the command's JSON output contains the resulting receipt(s).

See [the edge boundary](docs/kis-mock-edge.md) for the command, receipt, and
failure contract. Repository tests use fake transport responses and do not
place real VTS orders.

## What it does not do

This repository does **not** contain Go PG intents, gRPC, proto, or an
`auto_trader` modification/dependency. Its only live authorities are the
provider-pinned OAuth token endpoints documented above; it does not place live
orders or support order modification, market orders, Binance, or any paper
mutation beyond the specifically gated KIS domestic limit and Alpaca BTC/USD
limit placement boundaries plus cancellation of a durably ACCEPTED order.
Cancellation is available only through `POST /v1/commands/{command_id}/cancel`;
it is durably idempotent and cannot issue a second broker request.

Do not cite or integrate the orphan `auto_trader kis_websocket` Redis pub/sub
legacy path. OPSP8996 때문에 live WS shadow는 금지하며,
[fixtures/](fixtures/) is only a placeholder directory.

`execution_contracts/` provides the minimal command and receipt schema. Python은
지금 이 스키마로 호출하지 않는다.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
./scripts/secret_scan.sh
```
