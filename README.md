# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

It is a runtime, not a broker client: protocol code lives in separate libraries (for example, `go-kis`). Fillwire never places orders — it listens, persists, and delivers.

## Scope

PR1 implements the KIS execution reader, validation and normalization, a bounded hot-path channel, Redis Streams durability, and the execution-ledger ingest sink. The only KIS websocket subscription is the matched pair selected by configuration: live uses `EndpointLive` with `H0STCNI0`, and mock uses `EndpointVTS` with `H0STCNI9`. The KIS websocket carries fills only. The optional [quote reader](#quote-reader-off-by-default) is a separate lane on the Toss Securities websocket; it is off unless configured.

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

On shutdown, fillwire stops the socket reader first, then gives already received events a bounded `drain_timeout` window to pass through decode and `XADD`. The drain uses a separate background-rooted context rather than the canceled signal context. If the window expires, the remaining in-memory entries are logged and counted before exit.

Fillwire does not implement restart handoff, a reconcile-trigger client, a local checkpoint database, an HTTP metrics endpoint, heartbeat reporting, parallel consumers, broker orders, amendments, or cancellations. Those are outside this PR.

## Configuration

[`fillwire.toml`](fillwire.toml) is an example-only configuration. The ingest and KIS credentials are referenced by environment-variable name; their values never appear in the configuration or logs.

```toml
[kis]
broker = "kis"
endpoint = "live"
account_mode = "live"
approval_mode = ""
approval_refresh_margin = "1h"
venue = "krx"
hts_id = "EXAMPLE_HTS_ID"
app_key_env = "KIS_APP_KEY"
app_secret_env = "KIS_APP_SECRET"
event_buffer = 256
dup_track_max = 10000

[redis]
url = "rediss://redis.example.invalid:6380/0"

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
token_header = "X-Execution-Ledger-Ingest-Token"
batch_size = 200
timeout = "10s"

[channel]
buffer = 256
drain_timeout = "5s"

[retry]
min = "1s"
max = "30s"
factor = 2

[alerts]
rate_limit = "10m"
```

The default approval policy shares the Python at-kis-ws Redis approval contract. The
reference is auto_trader origin/main commit be0839808b35b7fcd4bbeb326ac0a84daf7ad96a,
files app/services/kis_websocket_internal/approval_keys.py and constants.py.
Live uses the raw-string cache key kis:websocket:approval_key and lock key
kis:websocket:approval_key:lock; mock uses kis_mock:websocket:approval_key and
kis_mock:websocket:approval_key:lock. The cache TTL is 23h (82800 seconds).
The lock uses SET NX EX with a unique token and 15s TTL; release is atomic
compare-and-delete. A contender waits at most 12s, rechecking every 0.25s
plus up to 0.25s jitter. It returns a transient failure if no sufficiently
live key appears, never issuing outside the lock. The approval REST issue call
has a 10s timeout, and the entire issue-and-publish operation has a 14s
deadline. Publishing checks lock ownership atomically; a late holder cannot
write or release a successor's lock. The provider reads Redis TTL rather than
assuming its local clock agrees with Redis.

The kis.approval_refresh_margin setting defaults to 1h and must be greater
than zero and no more than 2h. A key with Redis TTL
at or below that margin is refreshed under the same lock; a background loop
checks the TTL at least once per minute even if no resubscribe occurs.
Resubscribe also checks the margin. Racing timer and subscribe paths share the
lock and reuse the published key. A failed refresh while the old key still
has positive Redis TTL keeps the existing stream alive and retries in-process
with bounded jittered backoff. A resubscribe may use that still-live key after
a non-forced refresh failure. An absent or expired key remains a transient
process failure, and forced reissue never reuses a KIS-rejected key. The optional cache-only policy retains its
operator-controlled behavior: missing or empty cache exits 78 without REST.
Redis failure remains transient, including in cache-only mode.

Configuration errors, including missing KIS credentials and invalid settings,
exit 78 and the example systemd unit prevents restart for that code. Session
occupancy retains exit 42. Network, KIS 5xx, Redis, and lock-timeout failures
exit 1 so the unit retries under its restart-rate guard.

Telegram alerts use FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN and
FILLWIRE_ALERT_TELEGRAM_CHAT_ID. The desk owns the real values and wires them
only at deploy. If either is unset, one startup error log announces that
alerting is disabled. The alerts.rate_limit setting defaults to 10m per
failure class within the process. Delivery uses Telegram sendMessage with a
3s timeout; failure is logged without tokens and cannot alter the exit code.
Only fixed configuration and transient summaries are sent, never credentials,
approval keys, or exception bodies.

Structured logs count approval_rest_issue_call, approval_lock_acquired,
approval_lock_contended, approval_lock_wait_success, and
approval_lock_wait_failed. Each has account_mode and process-local cumulative
count fields. Task 180 should compare those counts with the Python service's
approval issue calls across the cache boundary. They are not durable counters
and reset on process restart.

Ingest URLs must use HTTPS, except HTTP is allowed only for `localhost`, `127.0.0.1`, or `::1`. Redis URLs must use `rediss`, except loopback `redis` and `unix` socket URLs. These checks run before startup so neither the ingest token nor the cached approval key can be sent over a remote plaintext connection.

If KIS returns `OPSP8996` (`ws.ErrSessionOccupied`) while subscribing, fillwire does not retry and exits with the named `ExitCodeSessionOccupied` code, `42`. This lets a future supervisor distinguish the session-holder condition from generic failure.

## Quote reader (off by default)

The quote reader subscribes the Toss Securities realtime websocket channels
`trade:kr`, `orderbook:kr`, `trade:us`, and `orderbook:us` for a configured
list of at most 40 symbols. It appends every tick to the Redis Stream
`quotes:toss`. It places no orders, applies no policy, and writes nothing to
the execution ledger. It is off unless `[quotes] enabled = true`. The KIS
websocket stays dedicated to `fills:kis`; the quote reader never uses KIS.

Protocol source: the Toss Open API realtime AsyncAPI document, version 1.2.2
(`https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json`). The
test fixtures in `internal/quote/testdata` follow it, and some are its own
examples.

### Contract: one Toss websocket connection

Toss allows **two websocket connections per account**. When a third opens,
Toss accepts it and closes the oldest one without a close code. So:

- fillwire's quote reader is the only Toss websocket client and uses exactly
  one connection. The second connection is a spare.
- **auto_trader never opens the Toss websocket.** Any other client that needs
  it must be agreed first, because two more connections would evict fillwire's.
- fillwire enforces the one connection in process: its dialer refuses a second
  dial while a connection is open, and every reconnect closes the old
  connection before dialing (break before make).

### Access token: read-only, never issued

The websocket handshake uses the same Bearer access token as the Toss REST
API. Toss allows one valid token per client, so a new token invalidates the
current one (auto_trader `app/services/brokers/toss/auth.py`). A token issued
or refreshed by fillwire would break the token auto_trader places orders
with. So fillwire:

- reads the one Redis key where auto_trader, or broker-edge gatewayd, caches
  the token: `toss:oauth:<sha256(client_id)[:16]>:access_token`, JSON with
  `access_token` and `expires_at`. It uses a single `GET` from its own Redis
  client. The key must be in the Redis at `[redis] url`.
- holds no Toss client id or secret, and has no code path to the token
  endpoint. A test scans every non-test Go file for the OAuth path, the REST
  host, and credential grant names, and pins the only Toss address to the
  websocket endpoint. This is the non-owner contract auto_trader already
  defines in `app/services/fill_watch_context/toss_token_boundary.py`.
- treats a token within 120 s of `expires_at` as absent, which is
  auto_trader's own buffer. With no usable token the reader does not dial; it
  re-reads the key at intervals that back off to at most 15 s.
- after a handshake 401, never redials with that same token. It waits until
  the owner publishes a different one.

The token is checked only at the handshake. Toss keeps an open connection
alive after the token expires, so a valid token matters only when connecting
or reconnecting.

**Enablement prerequisites.** These are decisions for desk and the operator;
they block enabling the reader, not the code:

1. **Token freshness owner.** Something must keep the cached token valid
   during the quote windows: KR 08:00–20:00 KST, and US 04:00–20:00 ET. Today
   auto_trader, or gatewayd in gatewayd mode, refreshes on demand only. If
   nothing refreshes, the reader stays idle, fails closed, and logs
   `token_unavailable`.
2. **Toss allowed IP.** The fillwire host must be on the Toss allowed-IP list,
   the same list REST uses. Otherwise the handshake returns 403 and the reader
   retries every 5 minutes.

### Isolation from the fills path

- The quote reader owns its websocket, its dialer, and its goroutines. It also
  owns a separate Redis client: a pool of 4 connections with 2 s timeouts. It
  reads only the token key and writes only its `quotes:` stream.
- It never feeds fillwire's exit decision. A socket failure, a Toss error
  frame, a missing token, a Redis failure, or a reconnect storm is logged and
  retried with backoff. Backoff starts at 1 s and doubles with jitter to
  5 min. While waiting for a token, the interval is capped at 15 s. Exit
  codes are unchanged.
- A panic in the lane, or in its dialer, transport, or token reader, is
  contained. It stops only the lane or becomes a retryable error.
- `[quotes]` is decoded strictly, in a separate pass. A bad table disables
  only the quote reader, with the log line `quote reader disabled:
  configuration rejected`. Bad tables include an unknown key such as
  `client_secret`, a wrong type, a malformed or over-long symbol list, or a
  bad `token_key`. A disabled table, `enabled = false` or no table at all, is
  not checked further and opens nothing.
- The fills code gains only insertions: the lane starts after the fills
  pipeline and is halted first at shutdown. Waiting for its socket to close
  is bounded at 7 s after the fills shutdown.

Tests start the real `runWithDependencies`, first with the quote lane off and
then with it on, under several conditions:

- healthy traffic
- a dial-failure storm
- a reconnect storm
- `server-shutdown` frames
- a refused token (401)
- a missing token
- a panic

In every case, these are identical to the off run: the `fills:*` stream
entries, the fills socket's requests, its single dial, and the fills approval
cache. The cached Toss token is never changed.

### Limits

The limits come from the AsyncAPI document:

- **100 subscriptions per connection.** Each symbol takes two
  (trade and orderbook), so the 40-symbol cap uses at most 80.
- **5 declarations per second.** The reader declares once per connection,
  spaced at least 1 s apart even across reconnects, and waits 1 s before
  redeclaring after `rate-limit-exceeded`.
- **Keepalive.** Toss closes a connection after 180 s with no client frame.
  The reader sends a text `PING` every 60 s.
- **Rejected subscriptions.** A rejected entry in the subscription ack (for
  example `stock-not-found`) is logged with its code and counted, and the
  accepted entries keep streaming.

### Lossy by design

Toss trade and orderbook frames carry no sequence number. The server may drop
frames and always favours the latest state. The reader adds its own loss
point too: when Redis is slow, ticks are dropped and counted rather than
stalling the socket. Use `quotes:toss` for triggers and touches. It is not a
record of every print, and cumulative volume cannot be rebuilt from it.

### Sessions

The `session` field comes from each tick's own timestamp and market. Every
window is Monday to Friday in the market's own time zone. Boundaries follow
auto_trader: `classify_kr_accept_session` for KR and `us_market_session` for
US. A window followed by a gap also includes its closing minute.

| session | window |
|---|---|
| `nxt_pre` | 08:00–08:50 KST |
| `krx_regular` | 09:00–15:30 KST |
| `nxt_after` | 16:00–20:00 KST |
| `us_pre` | 04:00–09:30 ET |
| `us_regular` | 09:30–16:00 ET |
| `us_after` | 16:00–20:00 ET |

- Daylight saving applies to the ET windows, using the embedded tzdata.
- Some ticks fall outside every window and are dropped and counted: those
  between 15:31 and 16:00 KST (NXT's after-market opens at 15:40), US
  day-market ticks, and anything on a weekend.
- Exchange holidays and early closes are not modelled.

The socket is open during the union of the configured markets' windows.
Gaps shorter than 30 minutes are merged, so a KR-only list keeps one socket
from 08:00 to 20:01 KST.

### Symbol list file

A plain-text file with one `<market> <code>` pair per line, where market is
`kr` or `us`:

- a KR code is six characters of `0-9` and `A-Z`
- a US code is an upper-case ticker of up to ten characters from `A-Z`,
  `0-9`, `.` and `-`, starting with a letter, as the Toss master spells it

Blank lines are ignored, and `#` starts a comment. The file must contain 1 to
40 entries, with no duplicates, and be at most 64 KiB. Anything else refuses
the quote reader (not fillwire) at startup. The desk builds it from current
holdings plus the H6 allowlist:

```text
# holdings
kr 005930
kr 000660  # SK hynix
# H6 allowlist
us AAPL
```

The file is read once at startup. Restart fillwire to apply a change.

### Stream entries

Each tick is one `XADD quotes:toss MAXLEN ~ <max_len> *` entry. Every entry
has exactly these fields, always all present:

| field | value |
|---|---|
| `symbol` | KR code or US ticker |
| `ts` | the frame's own timestamp, RFC 3339 with milliseconds, for example `2026-09-30T13:15:02.123+09:00` |
| `price` | trade price (`trade:*`); empty on orderbook ticks |
| `bid1`, `bid_qty` | best bid price and volume, from `bids[0]` (`orderbook:*`); empty on trade ticks or an empty side |
| `ask1`, `ask_qty` | best ask price and volume, from `asks[0]` (`orderbook:*`); empty on trade ticks or an empty side |
| `session` | one of the session labels above |

- **Numbers** are the Toss decimal strings as sent, after validation. No value
  is carried over from an earlier frame.
- **Dropped and counted:** malformed frames, unknown topics or symbols,
  orderbook frames with a null timestamp, and out-of-window ticks.

### Turning it on (desk)

1. Settle the two enablement prerequisites above.
2. Find the cached token key. On the Redis at `[redis] url`, run
   `redis-cli --scan --pattern 'toss:oauth:*:access_token'`. It prints the
   key name only. Do not `GET` it.
3. Write the symbol list file on the host, for example
   `/etc/fillwire/quote-symbols.txt`. Mount it read-only into the container
   by adding
   `--volume /etc/fillwire/quote-symbols.txt:/etc/fillwire/quote-symbols.txt:ro`
   to the unit's `docker run` line. No new environment variable is needed.
4. In the host TOML, set:

   ```toml
   [quotes]
   enabled = true
   provider = "toss"
   symbols_file = "/etc/fillwire/quote-symbols.txt"
   token_key = "toss:oauth:<16 hex>:access_token"
   stream_key = "quotes:toss" # default; must start with quotes:
   max_len = 100000           # default; XADD MAXLEN ~ N
   buffer = 1024              # default, at most 65536
   ```

5. Restart fillwire the usual way (`docs/digest-pin-deploy.md`). A restart also
   restarts the KIS fills socket, so use the same window you would for a
   deploy.

### Verifying

```sh
sudo journalctl -u fillwire.service -n 200 --no-pager | grep -E 'quote reader|quote subscription|KIS websocket initial subscription active'
# expected: "KIS websocket initial subscription active" (fills, unchanged)
#           "quote reader started" lane=quotes provider=toss symbols=<n>
#           during a window: "quote reader subscriptions active" subscribed=<2n> rejected=0
#           outside a window: "quote reader idle outside trading windows" next_open=...
# must not appear: "quote reader disabled"; investigate any "quote subscription rejected"
redis-cli -u "$REDIS_URL" XLEN quotes:toss
redis-cli -u "$REDIS_URL" XREVRANGE quotes:toss + - COUNT 3
# expected during a window: a growing length; entries with exactly the eight fields above
redis-cli -u "$REDIS_URL" XLEN fills:kis
# fills keep flowing exactly as before
```

Here `$REDIS_URL` is the desk's own Redis connection, typed in the shell and
not taken from this repository. At each window close, and at shutdown,
fillwire logs process-local counters. They include:

- frames and ticks written
- drops by reason
- rejected subscriptions and error frames
- `token_unavailable` and `token_rejected`

Growing `token_unavailable` means prerequisite 1 is not met. A handshake 403
in the retry log means prerequisite 2 is not met.

### Turning it off

Set `enabled = false`, or delete the `[quotes]` table, and restart fillwire.
The mount can stay or go. Existing `quotes:toss` entries remain until trimmed
by `MAXLEN` or deleted by the desk (`DEL quotes:toss`).

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

Authentication is a dedicated ingest token sent verbatim — no `Bearer` prefix — under the header named by `ingest.token_header`. The default is `X-Execution-Ledger-Ingest-Token`, matching the default of auto_trader's `EXECUTION_LEDGER_INGEST_TOKEN_HEADER` (auto_trader `app/core/config.py`; contract in its `docs/runbooks/execution-ledger-ingest.md`). If the server overrides that variable, set `token_header` to the same name — a mismatch makes every POST fail with 401. Exactly one header carries the token: no `Authorization` header is sent unless `token_header` is literally `Authorization`. The name must be a valid HTTP field name (RFC 7230 token); an invalid value fails at config load with exit 78. Configurations without `token_header` keep working unchanged.

The KIS raw websocket frame is not forwarded. `raw_payload_json` is a reparsable structured object containing `tr`, decrypted `fields`, and `received_at`, plus duplicate-hint metadata when applicable. It contains no approval key or ingest credential.

## Internal observations

PR1 exposes a named in-process counter snapshot for validation drops, duplicate suspects, successful XADDs, successful XACKs, and failed ingest attempts. It deliberately has no Prometheus dependency or endpoint.
