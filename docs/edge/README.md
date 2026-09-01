# broker-edge

`broker-edge` is a small Go bootstrap for a bounded KIS VTS read CLI. Today it
contains only `kis-mock-read`; Python will connect later over an approved HTTPS
boundary. `cmd/` follows Go convention only—Python does not import it.

## What it does

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

## Configuration and use

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

## What it does not do

This repository does **not** contain Go PG intents, a broker-gateway, gRPC,
proto, a live host, or an `auto_trader` modification/dependency. It does not
place, modify, cancel, or otherwise mutate orders; mutation TRs are absent from
the allowlist.

Do not cite or integrate the orphan `auto_trader kis_websocket` Redis pub/sub
legacy path. OPSP8996 때문에 live WS shadow는 금지하며,
[fixtures/](fixtures/) is only a placeholder directory.

`execution_contracts/` provides only the minimal future schema names. Python은
지금 이 스키마로 호출하지 않는다.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
./scripts/secret_scan.sh
```
