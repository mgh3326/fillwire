# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

It is a runtime, not a broker client: protocol code lives in separate libraries (for example, `go-kis`). Fillwire never places orders — it listens, persists, and delivers.

## Scope

PR1 implements the KIS execution reader, validation and normalization, a bounded hot-path channel, Redis Streams durability, and the execution-ledger ingest sink. The only KIS websocket subscription is the matched pair selected by configuration: live uses `EndpointLive` with `H0STCNI0`, and mock uses `EndpointVTS` with `H0STCNI9`. The optional [quote reader](#quote-reader-off-by-default) is a separate lane on its own socket and app key; it is off unless configured.

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

The quote reader subscribes KIS `H0STCNT0` (trade price) and `H0STASP0` (order
book) for a configured list of at most 40 KRX symbols and appends every tick to
the Redis Stream `quotes:kis`. It places no orders, applies no policy, and
writes nothing to the execution ledger. It is off unless `[quotes] enabled =
true`.

### Isolation from the fills path

KIS admits **one websocket session per app key**: a second socket on the same
key is refused with `OPSP8996` (go-kis `kis/ws` package documentation; the
auto_trader `docs/runbooks/ncp-pull-deploy.md` notes the same, and task 836
records at-kis-ws deploys failing with "appkey in use" while fillwire held the
session). A quote socket on the fills key would therefore either be refused or,
if it connected while the fills socket was between reconnects, take the
session and make fills exit 42. So the quote reader:

- requires its own app key and secret under environment names distinct from
  `[kis]`, and refuses to start when the quote key's value equals the fills
  key's value;
- issues its approval key through its own KIS REST client and caches it in
  process only. It never reads or writes the fills approval cache
  (`kis:websocket:approval_key*`) or its lock;
- owns its websocket, dialer, reconnect loop, Redis client (a separate pool of
  4 connections with 2s timeouts), and goroutines. A panic in the lane is
  contained and stops only the lane;
- never feeds fillwire's exit decision. A socket failure, `OPSP8996` on the
  quote key, a Redis failure, or a reconnect storm is logged and retried with
  backoff (5s doubling to 5m between window runs), and exit codes are
  unchanged;
- rejects a bad `[quotes]` table (wrong types, a malformed or over-long symbol
  list, a missing credential) by logging `quote reader disabled:
  configuration rejected` and running fills exactly as without the table.
  `[quotes]` is decoded in a separate pass, so it cannot fail the fills
  configuration.

Tests start the real `runWithDependencies` with the quote lane off and then on
under healthy traffic, a dial-failure storm, a go-kis reconnect storm,
`OPSP8996`, and a panic. In every case the `fills:*` stream entries, the fills
socket's requests, its single dial, and the fills approval cache are identical
to the off run.

**Limit not established by the repositories:** no source in fillwire, go-kis,
or auto_trader states how many realtime registrations one KIS session accepts.
40 symbols × 2 TRs is 80 registrations. A registration KIS refuses is logged
with its `msg_cd`, counted, and skipped; the accepted ones keep streaming. Check
`accepted` against `requested` in the log line below before relying on the full
list. This is one reason the reader ships disabled.

### Trading windows

The socket is open only Monday to Friday, KST, during KRX regular trading
09:00–15:30 and after-hours 16:00–20:00. Each window includes its closing
minute, so the socket closes at 15:31:00 and 20:01:00. Outside a window the
socket is closed with an unsubscribe for every registration. A tick whose own
exchange time falls outside both windows is dropped. Exchange holidays are not
modelled: on a holiday the socket opens and receives nothing. Whether KIS
publishes after-hours prints on these two TR ids has not been verified here.
If the 16:00–20:00 window stays empty on a trading day, check this first.

### Symbol list file

A plain-text file with one six-character KRX short code (`0-9`, `A-Z`) per
line. Blank lines are ignored, and `#` starts a comment that runs to the end of
the line. The file must contain 1 to 40 codes, with no duplicates, and be at
most 64 KiB. Anything else refuses the quote reader (not fillwire) at startup.
The desk builds it from current holdings plus the H6 allowlist:

```text
# holdings
005930
000660  # SK hynix
# H6 allowlist
0001A0
```

The file is read once at startup. Restart fillwire to apply a change.

### Stream entries

Each tick is one `XADD quotes:kis MAXLEN ~ <max_len> *` entry with exactly these
fields, always all present:

| field | value |
|---|---|
| `symbol` | KRX short code |
| `ts` | RFC 3339 KST time: the receipt date plus the frame's exchange `HHMMSS`, for example `2026-09-30T13:15:02+09:00` |
| `price` | last trade price (`H0STCNT0`); empty on order-book ticks |
| `bid1`, `ask1` | best bid and ask (`H0STASP0`); empty on trade ticks |
| `bid_qty`, `ask_qty` | best-level resting quantity (`H0STASP0`); empty on trade ticks |
| `session` | `regular` or `after_hours` |

Numbers are canonical non-negative decimal integers. No value is carried over
from an earlier frame. Field positions follow the only layout source in the
repositories, auto_trader `mock_scalping_ws/quote_protocol.py`. Because that
source does not map the trade TR's own bid and ask, trade ticks leave them
empty. Malformed, partial, unknown-symbol, and out-of-window records are
dropped and counted. The quote lane is lossy by design: when Redis is slow,
ticks are dropped and counted rather than stalling the quote socket.

### Turning it on (desk)

1. Obtain a KIS app key and secret **dedicated to this reader**. It must not
   be the fills key, and no other websocket client may use it (at-kis-ws,
   mock_scalping_ws, another fillwire). Choose `endpoint = "live"` or `"mock"`
   to match that key.
2. Add `KIS_QUOTE_APP_KEY` and `KIS_QUOTE_APP_SECRET` (or the names you set in
   `app_key_env` and `app_secret_env`) to the host env file. Never put their
   values in the TOML.
3. Write the symbol list file on the host, for example
   `/etc/fillwire/quote-symbols.txt`, and mount it read-only into the container
   by adding
   `--volume /etc/fillwire/quote-symbols.txt:/etc/fillwire/quote-symbols.txt:ro`
   to the unit's `docker run` line.
4. In the host TOML, set:

   ```toml
   [quotes]
   enabled = true
   endpoint = "live"
   app_key_env = "KIS_QUOTE_APP_KEY"
   app_secret_env = "KIS_QUOTE_APP_SECRET"
   symbols_file = "/etc/fillwire/quote-symbols.txt"
   stream_key = "quotes:kis" # default; must differ from [stream] key
   max_len = 100000          # default; XADD MAXLEN ~ N
   buffer = 1024             # default, at most 65536; ticks held for XADD before dropping
   ```

5. Restart fillwire the usual way (`docs/digest-pin-deploy.md`). A restart also
   restarts the fills socket, so use the same window you would for a deploy.

### Verifying

```sh
sudo journalctl -u fillwire.service -n 200 --no-pager | grep -E 'quote reader|KIS websocket initial subscription active'
# expected: "KIS websocket initial subscription active" (fills, unchanged)
#           "quote reader started" lane=quotes symbols=<n>
#           during a window: "quote reader subscriptions active" accepted=<2n> requested=<2n>
#           outside a window: "quote reader idle outside trading windows" next_open=...
# must not appear: "quote reader disabled"
redis-cli -u "$REDIS_URL" XLEN quotes:kis
redis-cli -u "$REDIS_URL" XREVRANGE quotes:kis + - COUNT 3
# expected during a window: a growing length; entries with exactly the eight fields above
redis-cli -u "$REDIS_URL" XLEN fills:kis
# fills keep flowing exactly as before
```

Here `$REDIS_URL` is the desk's own Redis connection, typed in the shell and
not taken from this repository. At each window close, fillwire logs `quote
reader window closed` with process-local counters: ticks written, and drops by
reason (malformed, partial, unknown symbol, out of window, buffer full, XADD
error, rejected subscription).

### Turning it off

Set `enabled = false` (or delete the `[quotes]` table) and restart fillwire.
The env vars and mount can stay or go. Existing `quotes:kis` entries remain
until trimmed by `MAXLEN` or deleted by the desk (`DEL quotes:kis`).

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
