# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

It is a runtime, not a broker client: protocol code lives in separate libraries (for example, `go-kis`). Fillwire never places orders — it listens, persists, and delivers.

## Scope

PR1 implements the KIS execution reader, validation and normalization, a bounded hot-path channel, Redis Streams durability, and the execution-ledger ingest sink. The only KIS websocket subscription is the matched pair selected by configuration: live uses `EndpointLive` with `H0STCNI0`, and mock uses `EndpointVTS` with `H0STCNI9`.

The pipeline is deliberately single-consumer at the sink:

```text
go-kis kis/ws
  -> reader drains Events()
  -> decode and normalize
  -> bounded channel (backpressure)
  -> XADD fills:kis MAXLEN ~ N
  -> XREADGROUP fillwire-ingest / one consumer
  -> execution-ledger ingest POST
  -> XACK inserted | updated | unchanged only

startup: XAUTOCLAIM pending entries before ordinary XREADGROUP
```

`rejected` results remain pending. Network failures, timeouts, 5xx responses, 4xx envelope failures, and response/result-length mismatches are retried with backoff and are never acknowledged speculatively.

Fillwire does not implement restart handoff, a reconcile-trigger client, a local checkpoint database, an HTTP metrics endpoint, heartbeat reporting, parallel consumers, broker orders, amendments, or cancellations. Those are outside this PR.

## Configuration

[`fillwire.toml`](fillwire.toml) is an example-only configuration. The ingest credential is referenced by environment-variable name; its value never appears in the configuration or logs. The KIS REST fallback likewise takes application credentials from environment-variable names.

```toml
[kis]
broker = "kis"
endpoint = "live"
account_mode = "live"
venue = "krx"
hts_id = "EXAMPLE_HTS_ID"
app_key_env = "KIS_APP_KEY"
app_secret_env = "KIS_APP_SECRET"
event_buffer = 256
dup_track_max = 10000

[redis]
url = "redis://redis.example.invalid:6379/0"

[stream]
key = "fills:kis"
max_len = 100000
consumer_group = "fillwire-ingest"
consumer_name = "fillwire-example"
claim_min_idle = "1m"
read_block = "5s"

[ingest]
url = "https://auto-trader.example.invalid/trading/api/execution-ledger/fills/ingest"
token_env = "EXECUTION_LEDGER_INGEST_TOKEN"
batch_size = 200
timeout = "10s"

[channel]
buffer = 256

[retry]
min = "1s"
max = "30s"
factor = 2
```

The approval provider reads `kis:websocket:approval_key` for compatibility with the existing cache but never writes that key. A cache miss delegates to the KIS REST approval provider; `Reissue` always bypasses the cache.

If KIS returns `OPSP8996` (`ws.ErrSessionOccupied`) while subscribing, fillwire does not retry and exits with the named `ExitCodeSessionOccupied` code, `42`. This lets a future supervisor distinguish the session-holder condition from generic failure.

## Idempotency key

`fill_seq` is derived from the KIS execution record alone: `sha256` over all decrypted
record fields joined with `^`, first 8 hex digits read as uint32, masked with `0x7FFFFFFF`.
It is computed once at decode time and carried on the stream entry, so a redelivery
recomputes nothing and the ledger idempotency key
`(broker, account_mode, venue, broker_order_id, fill_seq)` absorbs at-least-once delivery.
It is deliberately not byte-compatible with the Python monitor's fallback, which seeds
from normalized Python values.

Two partial fills of the same order that share quantity, price and second (HHMMSS) are
indistinguishable in the KIS record, which carries no fill sequence number. The second is
absorbed as `unchanged` by the ledger idempotency key. fillwire marks such an entry
`dup_suspect` with an observation count and raises a counter, but the authoritative
correction for this loss is the reconcile backfill against broker order history.

The duplicate hint is best-effort and process-local. It tracks `(broker_order_id, fill_seq)` only up to `dup_track_max`, evicting the oldest observation at capacity; it resets on restart. The hint is stored as separate Redis Stream fields and, for a suspect, inside `raw_payload_json`. It is never added as a top-level ingest field because that schema rejects unknown keys. It never changes `fill_seq` or `broker_order_id`.

## Ingest contract

The sink posts batches of 1–200 records to `/trading/api/execution-ledger/fills/ingest` with source `fillwire`. The endpoint described by auto_trader PR #2061 is still under verification and is treated here as a contract document only; this repository has no real-server test or call. The result array is positional. `inserted`, `updated`, and `unchanged` acknowledge their matching stream IDs; `rejected` does not.

The KIS raw websocket frame is not forwarded. `raw_payload_json` is a reparsable structured object containing `tr`, decrypted `fields`, and `received_at`, plus duplicate-hint metadata when applicable. It contains no approval key or ingest credential.

## Internal observations

PR1 exposes a named in-process counter snapshot for validation drops, duplicate suspects, successful XADDs, successful XACKs, and failed ingest attempts. It deliberately has no Prometheus dependency or endpoint.
