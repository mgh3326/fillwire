# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

It is a runtime, not a broker client: protocol code lives in separate libraries (for example, `go-kis`). `fillwire` never places orders: it listens, persists, and delivers. The separate
operator command [`execmock`](docs/e3a-mock-runbook.md) can send approved KIS mock
intents, with sending off by default.

## Scope

PR1 implements the KIS execution reader, validation and normalization, a bounded hot-path channel, Redis Streams durability, and the execution-ledger ingest sink. The only default KIS websocket subscription is the matched pair selected by configuration: live uses `EndpointLive` with `H0STCNI0`, and mock uses `EndpointVTS` with `H0STCNI9`. Two optional read-only lanes add subscriptions on that same session and never open a second connection: `[books]` adds one `H0STASP0` symbol, and `[kis_quotes]` adds promoted trade and book symbols. The optional [quote reader](#quote-reader-off-by-default) is a separate lane on the Toss Securities websocket; it is off unless configured.

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

Fillwire does not implement restart handoff, a local checkpoint database, an HTTP metrics endpoint, heartbeat reporting, parallel consumers, broker orders, amendments, or cancellations. Those are outside this PR.

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

The KIS `app_key_env`/`app_secret_env` pair is required only when
`approval_mode = ""` — fillwire then issues approval keys through the KIS REST
client — or when `[books] enabled = true`, whose REST reader sends both and
whose mock token-cache key derives from the app key. With
`approval_mode = "cache-only"`, `"gatewayd"` or `"gatewayd-fetch"` and books off, the names may be
omitted; a configured name whose variable is unset is not an error and the
value is never read.

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

`approval_mode = "gatewayd"` makes gatewayd the sole approval issuer for the
configured account mode (provider `kis-live-approval` or `kis-mock-approval`).
fillwire never issues a key locally and never touches the local issuance lock
in this mode. On a cache miss it POSTs `/v1/tokens/{provider}/ensure` on the
`[kis.gatewayd]` URL (`?force=true` on forced reissue), then polls Redis for a
usable shared key for about 3 seconds. The HTTP response is only an
acknowledgement — Redis stays authoritative — so an acknowledged ensure that
never produces a usable key is a typed
`ErrGatewaydApprovalUnavailable`, while a transport or ensure failure stays
`ErrApprovalUnavailable`. Forced reissue excludes the key this process last
presented — the one KIS rejected — and accepts any different live published
key (falling back to the cached value when nothing was presented). The ensure
POST has a 5s budget with a 3s connect bound, honors no proxy environment, and
follows no redirects. An optional `token_env`/`token_header` pair sends a
credential on that POST only; the value never appears in logs or errors, and a
configured credential requires an `https` URL or a loopback host so it is never
sent in cleartext. The `[kis.gatewayd]` table needs a valid `url` only in this
mode (or `"gatewayd-fetch"`, below) and its settings apply only in those modes
(plus the Toss `token_source` below), but unknown keys in the table
always reject the config.

`approval_mode = "gatewayd-fetch"` takes the approval key from gatewayd's
response and keeps it in memory. **It needs a gatewayd that serves
`POST /v1/tokens/{provider}/fetch` (hk 5760); deploy that gatewayd first.**
Against an older gatewayd the route does not exist and fillwire exits on the
first connect (see failure handling below). The call goes to the same
`[kis.gatewayd]` URL for `kis-live-approval` or `kis-mock-approval`, with body
`{}` or `{"failed_token_sha256": "<hex>"}`. In this mode:

- **No Redis for the key.** fillwire reads no approval key, takes no lock and
  never issues locally. The approval key exists only in process memory.
- **Refresh.** The key is read from memory while more than
  `approval_refresh_margin` of its life remains, with no HTTP call at all.
  Inside the margin, the next connect or reconnect asks gatewayd again. gatewayd
  reissues only when under 60 s remain, so a request inside the margin may
  legitimately return the same key; fillwire adopts it without an alert. There
  is no background refresh loop, as in `"gatewayd"` mode: the key matters only
  at the websocket handshake.
- **KIS rejection.** Where `""` mode forces a reissue, fillwire sends
  `failed_token_sha256`, the SHA-256 of the key KIS rejected. gatewayd reissues
  if that key is still current, and otherwise answers with the newer key it
  already holds; either answer is adopted. This covers every key reported
  through the websocket client's `Reissue` callback: that key is never presented
  again, and its hash stays pending across a failed or timed-out request.
  **Limitation:** the pinned go-kis client calls `Reissue` once per rejection
  code until a request is acknowledged. If KIS rejects key A with `OPSP0011`,
  gatewayd supplies B, and KIS rejects B with the same code before any
  acknowledgement, go-kis does not call `Reissue` again and fillwire has no
  signal for B. It keeps presenting B from memory while B has more than the
  margin left, so the fills stream stays disconnected and retrying until B
  nears expiry or the process restarts. A restart does not help by itself: its
  empty memory fetches without a hash, and gatewayd may return the same B.
  Treat repeated `OPSP0011` after a gatewayd-supplied key as an operator case
  (reissue at gatewayd). The first rejection at startup, before any
  acknowledgement, is handled by the same go-kis rule and is not changed here.
- **Failure.** A non-forced refresh failure (a timeout included) while the held key still has life
  keeps the stream, raises the transient alert and retries on the next connect.
  With no live key — and on every failed reissue, a timeout included — the failure is terminal
  (exit 1), with a typed error: `unauthenticated` (401), `provider_not_allowed`
  (403), `unknown_provider` (404), `bad_request` (400) and `issue_unavailable`
  (503) are distinct errors, and anything else is `gatewayd fetch unavailable`.
  fillwire never falls back to issuing locally or to reading Redis.
- **Expiry is trusted from gatewayd's `expires_at` against this host's clock**,
  not from a Redis TTL as in the other modes. The response's `provider`,
  `token_sha256` and `expires_at` are validated; a malformed answer or one
  already expired is `gatewayd fetch unavailable`.
- **Transport.** Same rules as the ensure call: 5 s budget, 3 s connect, no
  proxy environment, no redirects. The credential is **required**: `token_env`
  names the variable, whose value is sent as `Authorization: Bearer <value>`
  (the variable may hold the bare credential or `Bearer <credential>`; exactly one
  `Bearer` is sent, so a value already shared with `"gatewayd"` mode works), and
  `token_header` must be `Authorization` or unset. The credential needs an
  `https` URL or a loopback host. The key, the credential and gatewayd's response
  bodies never appear in logs or errors.
- **Rollback.** An image from before this release rejects `gatewayd-fetch` (exit
  78, no automatic restart) or the `token_source` key (quote reader disabled), so
  keep the previous `fillwire.toml` when you go back to an older pin.

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

## Reconcile trigger (off by default)

A fillwire restart or reconnect leaves a gap of a few seconds in which KIS fill notices are not received. auto_trader's own reconcile cron fills it later. The optional `[reconcile]` table makes fillwire ask auto_trader to reconcile right away, through the existing `POST /trading/api/execution-ledger/reconcile/trigger` endpoint. Fillwire only calls that endpoint: it makes no KIS REST call and writes no ledger rows.

```toml
[reconcile]
enabled = false          # default
url = "https://auto-trader.example.invalid/trading/api/execution-ledger/reconcile/trigger"
markets = ["kr"]         # default; fillwire subscribes only to domestic fills
dry_run = true           # default
interval = "0s"          # default: no periodic trigger; otherwise at least 1m
timeout = "10s"          # default
```

- **When it fires:** once after the initial KIS subscription is active, once after every successful reconnect, and every `interval` when that is set.
- **Request:** `{"market":"kr","dry_run":true,"reason":"reconnect"}` per market, with the token from `[ingest] token_env` in the `[ingest] token_header` header. The token value is never logged. `url` follows the ingest URL policy (HTTPS, or HTTP on loopback only) and must be an exact path with no credentials, query or fragment.
- **Off the fills path:** a trigger runs on its own goroutine. A slow, failing or 4xx/5xx trigger never delays decode, XADD or ingest, and never stops the process. Per market at most one request is in flight; triggers that arrive meanwhile coalesce into one follow-up request.
- **Responses:** redirects are not followed. The body must carry `status` of `executed`, `deduped` or `failed`; anything else is logged as a contract error. `deduped` (auto_trader allows one trigger per market per 60 seconds) is a normal outcome. Failures are counted and logged once per `[alerts] rate_limit` window per failure class, with the number of suppressed repeats. They do not send a Telegram alert.
- **Commit stays off until approved.** `dry_run = true` only reports what a backfill would do (`backfilled` is always 0). Setting `dry_run = false`, enabling the table on a host, and turning off auto_trader's 30-minute reconcile cron are separate, approved changes.

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

### Access token: read-only, never issued (Redis source)

This is the default `token_source = "redis"`. With `token_source =
"gatewayd-fetch"` fillwire asks gatewayd for the token instead; see
[Access token from gatewayd](#access-token-from-gatewayd-token_source--gatewayd-fetch)
for what changes.

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
  re-reads the key at intervals that back off to a 15 s base, plus up to 20%
  jitter, so a capped poll waits at most 18 s.
- after a handshake 401, does not redial with any token whose fingerprint it
  still holds. It keeps a bounded set of refused-token fingerprints: SHA-256
  hashes, never the token, the latest 32. It dials again only when the cache
  holds a token outside that set, so a refused A, then a refused B, then A
  back in the cache does not redial A.
  The bound is a deliberate trade-off. A token refused before 32 later
  refusals has been evicted, and it could be tried once more if the cache
  rolled back to it while its `expires_at` still looks usable. That attempt
  gets one more 401, the token is recorded again, and it does not repeat. It
  never issues a token and never touches fills.

The token is checked only at the handshake. Toss keeps an open connection
alive after the token expires, so a valid token matters only when connecting
or reconnecting.

**Enablement prerequisites.** These are decisions for desk and the operator;
they block enabling the reader, not the code:

1. **Token freshness owner.** Something must keep the cached token valid
   during the quote windows: KR 08:00–20:00 KST, and US 04:00–20:00 ET.
   auto_trader, or gatewayd in gatewayd mode, refreshes on demand only. The
   operator accepted on-demand refresh (Q-116 on task 1073).
   - **Behaviour when the token is stale.** When the cached token is missing,
     expired, or within 120 s of `expires_at`, the reader stalls and fails
     closed: it does not dial, and it issues nothing. On every attempt it logs
     the warning `quote reader window run failed; retrying` with reason
     `quote: no usable cached Toss access token`, and it re-reads the key at
     intervals that back off to a 15 s base, plus up to 20% jitter (at most
     18 s). The `token_unavailable` counter grows in the window-close and
     shutdown summaries.
   - **Resuming.** The reader resumes as soon as the owner's next on-demand
     refresh publishes a usable token.
   - **Open connection.** A connection that is already open is not affected,
     because Toss checks the token only at the handshake.
2. **Toss allowed IP.** The fillwire host must be on the Toss allowed-IP list,
   the same list REST uses. Otherwise the handshake returns 403 and the reader
   retries every 5 minutes.

### Access token from gatewayd (`token_source = "gatewayd-fetch"`)

Optional. gatewayd keeps the Toss token in its memory and returns it from
`POST /v1/tokens/toss/fetch` (hk 5760; **deploy a gatewayd with `/fetch`
first**). **Prerequisite: gatewayd must be the only issuer for that Toss
client.** Toss keeps one valid token per client, so any other issuer (auto_trader
issuing locally, a second gatewayd) invalidates the token gatewayd holds; a
handshake 401 would then make gatewayd reissue and invalidate the token the
other issuer uses for REST orders. fillwire's one-attempt-per-minute limit
cannot coordinate independent issuers. Before enabling, move auto_trader and any
other REST consumer of that client to gatewayd's token and turn their local
issuance off (tracked in hk 5766). In this mode fillwire:

- reads no Redis token key, so `token_key` must be absent. It uses the
  `[kis.gatewayd]` `url` and `token_env` (sent as `Authorization: Bearer`, https
  or loopback only; `token_header` must be `Authorization`) through its own
  client and its own in-memory holder, shared with nothing on the KIS path.
- keeps the token in memory and asks again only inside the same 120 s expiry
  buffer as above (gatewayd answers with the same token while more than 60 s
  remain), or after a handshake 401.
- after a 401, sends `failed_token_sha256` for the refused token, so gatewayd
  reissues or answers with the newer token it already holds. It makes at most
  one such reissue attempt per minute, failed attempts included, and the refused token's hash stays pending until gatewayd answers, because Toss keeps one valid token per client.
  This **changes the "never issued" contract above**: fillwire can now cause a
  reissue, though only gatewayd talks to Toss, and fillwire still holds no Toss
  client id or secret.
- trusts gatewayd's `expires_at` against its own clock.
- on any gatewayd failure (401, 403, 404, 400, 503, transport, timeout) does not dial, even while the held token still has life left; it
  retries at the same 15 s cadence as an empty cache, with a typed reason in the
  `quote reader window run failed; retrying` warning. Nothing falls back to
  Redis or to a local issue.
- never logs the token or the credential.

Settings are checked when the lane starts: a missing or invalid `[kis.gatewayd]`
URL, an unset credential variable, or `token_key` set alongside disables the
quote lane only (an error is logged) and never touches the fills path.

### Isolation from the fills path

- The quote reader owns its websocket, its dialer, and its goroutines. It also
  owns a separate Redis client: a pool of 4 connections with 2 s timeouts. It
  reads only the token key and writes only its `quotes:` stream.
- It never feeds fillwire's exit decision. A socket failure, a Toss error
  frame, a missing token, a Redis failure, or a reconnect storm is logged and
  retried with backoff. Backoff starts at 1 s and doubles with jitter to
  5 min. While waiting for a token, the base interval is capped at 15 s and jitter
  (up to 20%) is added on top, so one wait is at most 18 s. Exit
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
  pipeline and is halted first at shutdown. The wait for its socket to close
  has a 7 s deadline counted from that halt, running alongside the fills
  shutdown. It is checked only after the fills shutdown finishes, so it adds
  at most 7 s minus the time the fills shutdown took, and nothing once that
  reaches 7 s.

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
- **5 declarations per second.** The reader declares once per connection. A
  limiter admits at most five declarations in any rolling second, across
  reconnects. Every `rate-limit-exceeded` frame sets a cool-down when it is
  read, and no declaration is sent until at least 1 s after the latest such
  frame. This covers a late frame, a frame read while a declaration is
  already waiting, and the frame that exhausts the redeclarations and forces
  a reconnect.
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
  orderbook frames with a null timestamp or with both sides empty, and
  out-of-window ticks.

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

   To take the token from gatewayd instead of Redis, drop `token_key` and set
   `token_source = "gatewayd-fetch"` (see above). Do this only after gatewayd is
   the sole issuer for that Toss client and every REST consumer uses its token
   (hk 5766); otherwise the two issuers invalidate each other's token.

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

## KIS promoted quote lane (off by default)

The promoted quote lane registers the KIS realtime trade (`H0STCNT0`) and
order-book (`H0STASP0`) streams for a configured list of promoted symbols and
appends every tick to `quotes:kis:<symbol>`. It **shares the existing KIS
websocket session** that carries `fills:kis`; it never dials a second
connection, so the `OPSP8996` session-occupancy contract is unchanged. It
places no orders and writes nothing to the execution ledger. It is off unless
`[kis_quotes] enabled = true`.

### Registration limit (KIS realtime subscriptions)

Design 6321 §3.2 left the per-session registration limit unverified, and this
change could not confirm a number from an official KIS source either: the KIS
Developers portal (<https://apiportal.koreainvestment.com/docs>) requires a
login, and the public sample repository
(<https://github.com/koreainvestment/open-trading-api>) states no limit. The
default is therefore the conservative widely reported value **41
registrations per session**, marked **[unverified]**. Treat
`registration_limit` as a ceiling this process must not exceed, not as a
confirmed broker figure, until an official source states the number.

The lane spends that ceiling through the shared subscription budget in
`internal/ratebudget`:

```text
registration_limit 41  [unverified]
  - 1  execution notices (H0STCNI0, fixed)
  - 1  [books] H0STASP0, only when that lane is enabled (fixed)
  - 2  safety margin (always reserved)
  = 37 free registrations -> 18 promoted symbols (2 registrations each)
```

Fixed consumers reserve first and are never removed. A promoted symbol takes
two registrations; when the free slots cannot take another symbol, the
lowest-priority promoted symbol is removed first, and only after `min_hold`
has elapsed since its own admission. With no eligible symbol the promotion is
refused with `budget_cut` and nothing changes. The configured `symbols` order
is the priority order: the first entry is admitted first and survives longest.

### Feed-gap recovery

When a held symbol produces no websocket frame for `feed_gap`, the lane reads
the read-only REST quotations once per window and publishes the recovered
tick with `source = rest`:

- trades: `GET /uapi/domestic-stock/v1/quotations/inquire-ccnl`, transaction
  `FHKST01010300`, market `J` (the official sample:
  `examples_llm/domestic_stock/inquire_ccnl`).
- books: the same `FHKST01010200` asking-price read the book lane uses.

The exchange's own clock is kept; a REST response is not restamped as fresh.
A failed read publishes nothing.

### Configuration

```toml
[kis_quotes]
enabled = true
symbols = ["005930", "000660"]   # priority order, first = highest
registration_limit = 41           # [unverified] ceiling, default 41
min_hold = "15m"                  # default
feed_gap = "30s"                  # default
max_len = 100000                  # default, XADD MAXLEN ~ N
buffer = 1024                     # default, at most 65536
access_token_env = "KIS_MOCK_ACCESS_TOKEN"
# or: access_token_source = "redis"
```

As with `[books]`, exactly one token source is required: `access_token_env`
names an environment variable read on each REST recovery, or
`access_token_source = "redis"` reads the token auto_trader publishes under
the `token_keys.py` contract. The lane never issues a token. Both
`app_key_env` and `app_secret_env` are then required, because the REST reads
send them. `[kis_quotes]` is decoded strictly; a rejected table or a lane that
cannot start logs one error and leaves the fills pipeline running. NXT and the
combined-venue TRs are out of scope until their transaction IDs are confirmed.

### Stream entries

Each tick is one `XADD quotes:kis:<symbol> MAXLEN ~ <max_len> *` entry. Every
entry carries this fixed field set; fields that do not apply are empty.

| field | value |
|---|---|
| `symbol` | six-character KR issue code |
| `kind` | `trade` or `book` |
| `source` | `ws` or `rest` |
| `feed` | transaction that carried it: `H0STCNT0`, `H0STASP0`, `FHKST01010300`, `FHKST01010200` |
| `venue` | `KRX` |
| `ts_exchange` | exchange clock, RFC 3339 with offset, nearest KST day (KIS sends HHMMSS only) |
| `ts_received` | receive instant, RFC 3339 with offset |
| `price`, `qty`, `acml_vol` | trade ticks only, exact decimal strings; `side` is `buy`, `sell`, `auction`, or empty on REST ticks |
| `bid_price_1`…`bid_qty_5`, `ask_price_1`…`ask_qty_5` | book ticks only, best first, exact decimal strings |

Under backpressure the lane drops ticks from its bounded queue and counts
them; the fills path is never blocked by a quote write.

### Verifying

```sh
redis-cli -u "$REDIS_URL" XLEN quotes:kis:005930
redis-cli -u "$REDIS_URL" XREVRANGE quotes:kis:005930 + - COUNT 3
# expected during a session: a growing length; the fixed fields above
redis-cli -u "$REDIS_URL" XLEN fills:kis
# fills keep flowing exactly as before
```

At shutdown the lane logs `KIS 승격 시세` counters (published, dropped, gap
detections) and removes its subscriptions on the same connection.

## Book recorder and replay (`bookrec`)

`internal/book` records KIS domestic order-book (`H0STASP0`) and trade
(`H0STCNT0`) websocket frames into append-only evidence files, and replays
those files deterministically into book state and a trade tape. `cmd/bookrec`
is its command-line front end. Neither has a network client, and neither is
wired into the `fillwire` binary.

### Recording

`book.Recorder` takes frames from whatever reads the socket:

```go
f, _ := book.OpenEvidence("005930-20261102.evidence.jsonl") // append-only
rec, _ := book.NewRecorder(f, book.DefaultStaleAfter, time.Now())
rec.Frame(event.Raw, event.ReceivedAt) // each go-kis ws.Event
rec.Reconnect(info.At, info.Attempt)   // from ws.Config.OnReconnect
rec.Tick(time.Now())                   // every 500 ms
```

`bookrec record -out FILE < frames` does the same for raw frames piped one
per line on stdin, stamping each with its read time. The optional [live book producer](docs/book-feed.md) subscribes `H0STASP0`
inside fillwire's existing KIS session and writes `book:kis:<symbol>` to Redis.
It does not open another session or change the offline evidence format. Mock
mode has a read-only REST quotation fallback. Both the producer and execmock's
send mode remain off by default.

Each evidence line is one JSON object written with a single `write`:

| `kind` | Meaning |
|---|---|
| `session` | A recording run started; `stale_after_ms` is its threshold. |
| `frame` | One received frame, verbatim in `raw` (or base64 `raw_bytes` if not UTF-8), with `at_ns` and one verdict per record. A frame that does not parse keeps its `error` and changes nothing. |
| `reconnect` | The socket reconnected. Every book is unsynced until its next snapshot. |
| `stale` | A stream (`tr`, `symbol`) was silent for longer than the threshold (2 s by default) since `since_ns`. Flagged once per silence, by `Tick` or by the frame that ends the silence. |

KIS book and trade frames carry no sequence number, so the verdicts come from
the data:

- **trade gap**: `ACML_VOL - CNTG_VOL` is larger than the previous
  `ACML_VOL`; `missing_vol` is the difference. The trade is applied and
  becomes the new baseline.
- **duplicate**: a record identical to the last applied one, or a trade whose
  `ACML_VOL` was already applied. Skipped.
- **regress**: a trade whose `ACML_VOL` went backwards, or a book with an
  earlier exchange time. Skipped.
- **resync**: the first record per symbol after a reconnect. A book is applied
  even if identical; a trade is checked against the pre-reconnect volume, so
  trades lost during the outage appear as a gap. If `ACML_VOL` restarted (a
  new trading day), the tape restarts from it.
- **new exchange day**: a frame received on a later KST date than the one
  before it resyncs every book and restarts every symbol's tape baseline, even
  when the socket never reconnected.

Execution notices (`H0STCNI0`/`H0STCNI9`), subscribe acknowledgements and
PINGPONG frames are never recorded.

### Consuming the replay

An executor or backtest reads evidence through `book.Replay`. It recomputes
every verdict from the raw frames with the recorder's own detection code, so
the output depends only on the file's bytes: replaying the same file twice
yields byte-identical events.

```go
f, _ := os.Open("005930-20261102.evidence.jsonl")
state, err := book.Replay(f, func(e book.Event) error {
	switch e.Type {
	case book.EventBook:  // e.Book: full ten-level snapshot, e.Book.Synced
	case book.EventTrade: // e.Trade: one applied trade, in tape order
	case book.EventGap:   // e.MissingVol shares were traded but never received
	case book.EventStale, book.EventReconnect, book.EventSession:
	}
	return nil // a non-nil error stops the replay and is returned
})
// state.Books: final book per symbol; state.Tape: every applied trade;
// state.Summary: counts of gaps, duplicates, stale streams, torn lines.
```

`bookrec replay -in FILE [-out FILE]` writes the same events as JSON lines,
then the final book of each symbol, and prints the summary to stderr. It
refuses an `-out` that names the `-in` file.

Rules for a consumer:

- Act only on a book with `Synced: true`. After a `reconnect`, a new
  `session` or a new exchange day, a symbol's book is unsynced until its next
  `book` event.
- Treat a `gap` as missing tape: volume-based fill models must not assume
  those trades happened at a known price.
- Treat `stale` as no information since `since_ns`, not as an unchanged
  market.
- `summary.torn_lines` counts the torn last line of a crashed recorder;
  replay skips it. An invalid line anywhere else (one not followed directly
  by a new `session` line) fails the replay with `book.ErrCorrupt` rather than
  silently dropping frames. A non-zero `verdict_mismatches` means the file was
  written by different detection code; replay applies its own verdicts.
- Event times are receive times (`at_ns`, Unix ns); `hour` is the exchange
  time (HHMMSS, KST, one-second resolution).

### Build guarantee

`cmd/bookrec/noorder_test.go` builds the binary and fails if it links any
non-standard package outside an allowlist (`internal/book` and the
single-constant `internal/kst` only), links
`net`, `net/http` or `crypto/tls`, has a symbol shaped like an order client,
or contains a KIS order path (`/trading/order…`) or order transaction ID.

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

## Exit codes

Every binary (`fillwire`, `execmock`, `execsim`, `bookrec`) uses the one table in
`internal/exitcode`:

| Code | Meaning |
|---|---|
| 0 | clean exit |
| 1 | runtime failure a retry or restart may clear (network, Redis, broker 5xx, lock timeout, unreadable input) |
| 2 | bad command line: unknown flag, missing or conflicting arguments; nothing was read or sent |
| 42 | KIS session occupied (`OPSP8996`), `fillwire` only |
| 78 | configuration error: invalid settings, missing credentials, or a policy refusal (for example a non-mock `execmock` account, or plaintext Redis to a non-loopback host) |

Loopback and Redis URL rules come from `internal/netpolicy`, so every binary accepts and
refuses the same addresses.

## Golden files

Tests share one `-update` flag (`internal/testutil`). Some fixtures feed other packages,
so regenerate in this order and review the diff:

```bash
go test ./internal/exec/sim -update
```

```bash
go test ./cmd/execsim -update
```

```bash
go test ./cmd/execmock ./internal/book -update
```

## Internal observations

PR1 exposes a named in-process counter snapshot for validation drops, duplicate suspects, successful XADDs, successful XACKs, and failed ingest attempts. It deliberately has no Prometheus dependency or endpoint.

## KIS 승격 종목 시세 (기본 비활성화)

`[kis_quotes] enabled = true`로 켜면 승격 종목의 체결(`H0STCNT0`)·호가(`H0STASP0`)를 기존 KIS 웹소켓 세션에 등록하고 `quotes:kis:<symbol>` Redis Stream으로 출력합니다. 두 번째 세션을 열지 않으며 주문 경로가 없습니다. 세션당 실시간 등록 상한은 공식 문서(`https://apiportal.koreainvestment.com/docs`, 로그인 필요)와 공개 샘플 저장소에서 숫자를 확인하지 못해 보수값 41을 `[unverified]`로 두었습니다. 체결통보 1건과 `[books]` 1건, 안전 여유 2건을 먼저 예약하고 남는 등록만 승격 종목(종목당 2건)에 주며, 슬롯이 모자라면 `min_hold`(기본 15분)가 지난 가장 낮은 우선순위 종목을 먼저 해제합니다. 수신 공백이 `feed_gap`(기본 30초)을 넘으면 읽기 전용 REST 시세로 한 번 복구하며, 실패하면 아무것도 쓰지 않습니다.

## broker-edge now lives here

The `broker-edge` repository (`mgh3326/broker-edge` @ `0bdec8e`) was imported into this repository with its history (hk 5872). Its commands are `cmd/{edge-canary,gatewayd,kis-mock-edge,kis-mock-read,market-tap}`; the packages are under `internal/{canary,gatewayd,kislive,kismockedge,kismockread}` plus `execution_contracts/`. Behaviour and flags are unchanged. `Dockerfile.edge` builds the same four-binary image that `broker-edge` published, until the deploy cut-over (hk 5874).

Its documents moved:

- [`docs/edge/README.md`](docs/edge/README.md): the former `broker-edge` README (commands, environment, gatewayd, kis-mock-edge).
- [`docs/edge/`](docs/edge): `kis-mock-edge.md`, `canary.md`, `digest-pin-deploy.md`.
- [`deploy/edge/`](deploy/edge): systemd units, observability, `.env.example`, container-unit notes.
- [`scripts/edge/`](scripts/edge) and [`fixtures/edge/`](fixtures/edge).

## 시세 기록기 (기본 비활성화)

Upbit 공개 웹소켓 기록기는 API 키 없이 최대 20개 KRW 마켓의 `orderbook`(15개 호가 단위, 상위 10개 활용 가능) 및 `trade` 프레임을 수신합니다. `[upbit] enabled = true`를 설정하고 `markets_file`에 한 줄마다 `KRW-BTC`와 같은 마켓 코드를 적은 뒤 서비스를 재시작하세요. 기록은 `<directory>/upbit/<UTC 날짜>/<마켓>.jsonl`에 추가되며 UTC 날짜가 바뀌면 이전 파일을 동기화하고 gzip으로 닫습니다. 연결 단절 및 재연결은 `gap`과 `connected` 레코드로 남습니다.

Toss `[quotes]` 또는 KIS `[books]`를 켤 때도 Redis 보존 한도를 넘는 기록이 필요하면 `[recording] enabled = true`와 `directory`를 설정하세요. Toss 데이터는 `toss/`, KIS 호가 이벤트는 `kis/` 아래 같은 UTC 날짜 구조로 저장됩니다. 기존 Redis 스트림 출력은 계속 동작합니다. KIS 다중 마켓 호가 기록은 후속 작업입니다.

하루별 연결 커버리지를 확인하려면 다음 명령을 사용하세요. 요약은 마켓별 커버된 초, 하루 86,400초, 비율을 JSON으로 출력합니다.

```sh
fillwire coverage --dir /var/lib/fillwire/recordings --day 2026-11-10
fillwire coverage --dir /tmp/fillwire-recordings --day 2026-11-10 --from 2026-11-10T01:00:00Z --to 2026-11-10T01:05:00Z
```

Upbit 기록기를 단독으로 실행해 확인하려면 다음 명령을 사용하세요.

```sh
fillwire upbit --markets-file /etc/fillwire/upbit-markets.txt --dir /var/lib/fillwire/recordings
```

## 재연결 reconcile 트리거 (기본 비활성화)

`[reconcile] enabled = true`로 켜면 KIS 최초 구독 직후, 재연결 성공 직후, 그리고 `interval`(설정 시)마다 auto_trader의 `reconcile/trigger` 엔드포인트를 호출해 수 초의 체결 통지 공백을 바로 메웁니다. 토큰은 `[ingest]`와 같은 것을 쓰며 로그에 남지 않습니다. 호출은 체결 처리 경로와 분리되어 있어 느리거나 실패해도 처리·종료에 영향이 없습니다. 기본값 `dry_run = true`는 보고만 하며, `dry_run = false`(커밋) 전환과 auto_trader의 30분 TaskIQ 크론 해제는 별도 승인 후의 환경 변경입니다.

## broker-edge 통합

`broker-edge` 저장소(`mgh3326/broker-edge` @ `0bdec8e`)를 이력째 이 저장소로 들여왔습니다(hk 5872). 명령은 `cmd/{edge-canary,gatewayd,kis-mock-edge,kis-mock-read,market-tap}`, 패키지는 `internal/{canary,gatewayd,kislive,kismockedge,kismockread}`와 `execution_contracts/`에 있고 동작과 플래그는 바뀌지 않았습니다. 배포 전환(hk 5874) 전까지 `Dockerfile.edge`가 `broker-edge`가 게시하던 것과 같은 4개 바이너리 이미지를 만듭니다. 문서는 [`docs/edge/README.md`](docs/edge/README.md)와 `docs/edge/`, `deploy/edge/`, `scripts/edge/`, `fixtures/edge/`로 옮겼습니다.
