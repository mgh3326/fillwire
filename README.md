# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

Protocol code lives in libraries such as `go-kis`. The `fillwire run` and
`fillwire serve` runtimes listen, persist, and deliver. The operator command
[`fillwire execmock`](docs/e3a-mock-runbook.md) can send approved KIS mock
intents, with sending off by default.

## Scope

PR1 implements the KIS execution reader, validation and normalization, a bounded hot-path channel, Redis Streams durability, and the execution-ledger ingest sink. The only default KIS websocket subscription is the matched pair selected by configuration: live uses `EndpointLive` with `H0STCNI0`, and mock uses `EndpointVTS` with `H0STCNI9`. Three optional read-only lanes add subscriptions on that same session and never open a second connection: `[books]` adds one `H0STASP0` symbol, `[kis_quotes]` adds promoted trade and book symbols, and [`[kis_futures]`](#kis-지수선물-1분봉-기본-비활성화) adds index futures trade streams. The optional [quote reader](#quote-reader-off-by-default) is a separate lane on the Toss Securities websocket; it is off unless configured.

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

## 통합 CLI와 상주 서버

`go build -o fillwire ./cmd/fillwire`로 빌드합니다. `fillwire <명령> --help`에서
명령별 사용법을 확인할 수 있습니다. 기존 `fillwire -config FILE` 실행은
`fillwire run -config FILE`과 같습니다. 각 명령의 독립 실행 파일도 빌드할 수 있습니다.

`fillwire serve -config FILE`는 하나의 TOML 파일로 상주 서버와 모듈을 설정합니다.
아래 표의 각 모듈은 `enabled = true`를 지정하면 실행됩니다.

| 설정 표 | 기능 |
|---|---|
| `[gatewayd]` | 토큰 발급 및 조회 HTTP 서버. `providers`는 기존 CLI와 같은 쉼표 구분 목록이며 기본값은 `kis-mock`입니다. `ensure_interval`을 생략하면 요청 시에만 토큰을 발급합니다. `listen`과 `[redis].url`을 생략하면 각각 `GATEWAYD_LISTEN_ADDR`와 `REDIS_URL`을 사용합니다. 공급자 및 조회 인증은 기존 gatewayd 환경 변수를 사용합니다. |
| `[kis_fills]` | KIS 체결 기록과 ingest. `[kis]`, `[redis]`, `[stream]`, `[ingest]`, `[channel]`, `[retry]`를 사용합니다. |
| `[kis_quotes]` 또는 `[books]` | KIS 승격 종목 체결·호가 또는 고정 종목 호가. `[recording]`을 활성화하면 고정 종목 KIS 호가도 JSONL로 기록합니다. `fillwire bookrec`는 표준 입력 기록과 파일 재생을 제공합니다. |
| `[kis_futures]` | KIS 지수선물 웹소켓 분봉. live 세션에서만 동작합니다. |
| `[upbit]` | Upbit 공개 호가·체결 웹소켓 기록. |
| `[toss_ws]` | Toss `personal:order` 그림자 기록. |
| `[upbit_fills]` | Upbit 개인 체결 그림자 기록. |
| `[alpaca]` | Alpaca paper 체결 및 IEX 시세 기록. |
| `[binance_demo]` | Binance Spot Demo 시장·사용자 스트림 기록. |
| `[quotes]` | 기존 Toss 해외 종목 시세 피드. |

KIS 체결·호가·선물은 `[kis]`에 설정한 웹소켓 세션을 공유합니다. 호가 또는 선물만
활성화할 때는 체결 구독과 ingest 설정이 필요 없습니다. 선물을 함께 활성화하면
승격 호가의 등록 상한에서 선물 슬롯을 먼저 확보합니다. 서비스 모듈은 읽기와 기록을
담당하며 gatewayd는 토큰을 발급합니다. 주문 실행 명령은 CLI에서 별도로 실행합니다.

```toml
[serve]
health_listen = "127.0.0.1:8792"

[kis_fills]
enabled = true
```

`GET /healthz`에는 모듈별 `state`와 `restarts`가 표시됩니다. 활성 모듈이 모두 실행
중이면 HTTP 200입니다. 활성 모듈이 시작 중이거나 재시작 대기 중이거나
`failed` 상태이면 HTTP 503입니다. KIS 세션 점유와 cache-only 승인키의 영구 실패,
Toss 개인 스트림의 인증 중단은 재시도하지 않습니다. gatewayd는 별도 리스너에서
기존 `GET /healthz`를 제공합니다.
`kis_session`은 KIS 모듈이 사용하는 공유 세션입니다. 실행 상태는 프로세스 내부의
모듈 상태를 나타냅니다. 시세 신선도와 업스트림 API 가용성은 기존 모듈 로그를 확인합니다.
모듈이 종료되면 해당 모듈을 1초부터 최대 1분까지 지수 백오프로 다시 시작합니다.

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
  Inside the margin, the next connect or reconnect asks gatewayd again.
  Gatewayd refreshes keys with at most two hours left, which covers every
  supported reader margin. As in `"gatewayd"` mode, the key is needed at the
  WebSocket handshake.
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

### Toss 개인 주문과 시세 프레임을 같은 연결에 기록

`[toss_ws]`는 개인 주문용 웹소켓 연결 하나에 선택한 Toss 체결(`trade`)과
호가(`orderbook`) 구독도 함께 선언합니다. `symbols_file`은 `kr 005930`처럼
시장과 코드를 한 줄에 하나씩 적으며 최대 40개를 받습니다. 설정은 기본
비활성화입니다. 개인 주문 구독 1개와 시세 구독 심볼당 2개를 합쳐 Toss의
연결당 100개 한도를 넘으면 설정을 거부합니다.
`fillwire serve`에서 `[toss_ws] enabled = true`로 켜며, 개인 주문과 시세
구독은 같은 Toss 연결을 사용합니다.

주문 프레임은 `directory/toss-order/<UTC 날짜>/<account_seq>.jsonl`에,
시세 프레임은 `directory/toss-quote/<UTC 날짜>/<account_seq>.jsonl`에
`received_at`(로컬 수신 시각), `kind`, `topic`, 세션 ID, 원본 JSON과 함께
추가됩니다. 시세 기록은 별도 bounded queue와 기록기를 사용해 날짜 압축이
주문 프레임 수신을 막지 않게 합니다. 구문이 잘못됐거나 구독 심볼과 맞지 않는
프레임은 버리고 malformed 카운터를 올립니다. 이 기능은 Redis 시세 스트림이나
원장에 쓰지 않고 주문도 내지 않습니다.

Protocol source: the Toss Open API realtime AsyncAPI document, version 1.2.2
(`https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json`). The
test fixtures in `internal/quote/testdata` follow it, and some are its own
examples.

### Contract: one Toss websocket connection

Toss allows **two websocket connections per account**. When a third opens,
Toss accepts it and closes the oldest one without a close code. So:

- `[quotes]` uses one quote-reader connection. `[toss_ws]` uses one connection
  for `personal:order` and its optional trade/orderbook frames. If both are
  enabled, they use the two connections Toss allows; the order and quote
  channels in `[toss_ws]` do not open another connection.
- **auto_trader never opens the Toss websocket.** Any other client that needs
  it must be agreed first, because a third connection would evict the oldest.
- Each fillwire reader reconnects by closing its previous connection before
  dialing again (break before make).

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

## NHPLUG 모의 WebSocket 기록 (기본 비활성화)

`fillwire nhplug-ws -config fillwire.toml`은 NHPLUG 모의 WebSocket 한 세션에서
시세와 d0–d3 통보 프레임을 JSONL로 기록합니다. 이 명령은 주문이나 원장 경로를
호출하지 않습니다. 사용자 ID와 시세 채널은 설정에 두고, 앱 키와 시크릿은
`NHPLUG_APP_KEY`, `NHPLUG_APP_SECRET` 환경 변수에서 읽습니다. WebSocket 주소는
`wss://moapi.nhplug.com:17070/websocket`으로 고정되며 운영 WebSocket 포트는 거부합니다.
인증 토큰은 NH SDK 방식으로 발급하고 메모리에만 유지합니다.

```toml
[nhplug_ws]
enabled = false
user_id = "NHPLUG_USER_ID"
directory = "./recordings"
app_key_env = "NHPLUG_APP_KEY"
app_secret_env = "NHPLUG_APP_SECRET"

[[nhplug_ws.quotes]]
tr_cd = "mc"
tr_key = "005930"
```

serve 모듈 레지스트리가 아직 통합되지 않은 상태에서는 명령을 직접 실행합니다.
레지스트리 연결은 `[nhplug_ws]`를 읽고 `runNHPlugWS(ctx, cfg, tokens.Token)`을
실행하는 모듈 팩토리 한 줄로 연결할 수 있습니다.

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
two registrations, except a symbol whose book stream `[books]` already holds:
for that symbol the lane adds only its trade stream and still publishes both
trade and book ticks to `quotes:kis:<symbol>` (the book frame arrives through
the fixed subscription). When the free slots cannot take another symbol, the
lowest-priority promoted symbol is removed first, and only after `min_hold`
has elapsed since its own admission. With no eligible symbol the promotion is
refused with `budget_cut` and nothing changes. A failed unsubscribe keeps the
symbol and its slot, so the lane never counts a server registration it no
longer holds as free. The configured `symbols` order is the priority order:
the first entry is admitted first and survives longest.

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
| `price`, `qty`, `acml_vol` | trade ticks only, exact decimal strings; `acml_vol` is empty on a REST-recovered trade because that read carries no cumulative volume; `side` is `buy`, `sell`, `auction`, or empty on REST ticks |
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

## KIS 지수선물 1분봉 (기본 비활성화)

`[kis_futures] enabled = true` 로 켜면 KOSPI200 선물(`F200`)과 미니 KOSPI200 선물(`MF200`)의
근월·차월 **주간·야간 체결**을 받아 닫힌 1분봉을 Redis Stream 또는 JSONL 에 씁니다
(hk 6424, 설계 `docs/design/6321-go-quote-collector.md`). 읽기 전용입니다. 주문·원장·
research ingest 경로가 없으며, 정적 테스트(`internal/kisfutures/noorder_test.go`)가 링크하는
소스에 주문 경로·주문 TR 이 없음을 검사합니다.

### TR id 와 확인 상태

공식 샘플 저장소 <https://github.com/koreainvestment/open-trading-api> (commit `b0a0d33`)의
`examples_llm/domestic_futureoption/` 에서 확인했습니다.

| 세션 | TR | 샘플 |
|---|---|---|
| 주간 | `H0IFCNT0` | `index_futures_realtime_conclusion` (지수선물 실시간체결가 [실시간-010]) |
| 야간 | `H0MFCNT0` | `krx_ngt_futures_ccnl` (KRX야간선물 실시간종목체결 [실시간-064]) |

두 샘플의 앞 11개 열(`futs_shrn_iscd`, `bsop_hour`, …, `futs_prpr`, …, `last_cnqn`,
`acml_vol`)만 읽습니다. 주간 샘플은 실전 계좌만 지원한다고 적고 있어 레인은
`kis.endpoint = "live"` 에서만 켜집니다.

공식 근거로 확인하지 못해 **[unverified]** 이며, 켠 뒤 첫 smoke 에서 확인할 항목:

- 야간 구독 `tr_key` 형식. 레인은 마스터 단축코드(예: `A01612`)를 씁니다. 야간 샘플의
  예시 값은 `101W9000` 입니다.
- 자정 이후 `bsop_hour` 표기(00~05 시와 24~29 시를 모두 받습니다).
- `H0IFCNT0`/`H0MFCNT0` 가 미니 선물도 전달하는지, 모든 체결을 하나씩 보내는지.
- 세션당 등록 상한 41 ([KIS 승격 레인](#kis-promoted-quote-lane-off-by-default) 과 같은 값).

### 세션과 구독 예산

기존 KIS 웹소켓 세션(`fills:kis` 와 같은 연결)에 등록하며 두 번째 세션을 열지 않습니다.
등록은 4계약 × 2 TR = **8건**이고 `internal/ratebudget` 예산에서 예약합니다. 순서는 체결통보 →
`[books]` → 안전 여유 2건 → 선물 8건 → 승격 종목이므로, 상한 41 에서 `[books]` 를 켜면 승격
종목은 18개에서 14개가 됩니다. 상한은 `[kis_quotes] registration_limit` 을(그 레인이 꺼져
있으면 41 을) 씁니다. 8건을 예약할 수 없으면 선물 레인만 꺼집니다.

### 계약과 세션 규칙 (Python 과 동일)

규칙은 auto_trader `68b7ca593` 의 `kis_futures_quotes/contracts.py` 와
`research_candles/session_segment.py` 를 옮겼고, 그 Python 테스트의 사례를
`internal/kisfutures/testdata/python_rules.golden.json` 에 두었습니다.

- 계약은 공개 KIS 선물 마스터(`fo_idx_code_mts.mst.zip`, 인증 없음)에서만 정합니다. 계약
  날짜가 바뀔 때마다 다시 읽고, 실패하면 이미 읽은 행을 쓰며 1분 뒤 다시 시도합니다.
- 근월·차월을 정하는 날짜는 진행 중이거나 다음에 올 세션의 `session_date` 입니다. 주간 마감
  (15:45) 뒤에는 다음 거래일이므로, 최종거래일 저녁에는 만기 계약을 해제한 다음 새 계약을
  등록합니다.
- `session_segment`: 거래일의 08:45~15:45 는 `FUT_DAY`(`session_date` 그날), 18:00~06:00 은
  `FUT_NIGHT`(`session_date` 는 야간이 **시작한 날의 다음 XKRX 거래일**), 그 밖이나 판정할 수
  없으면 `UNKNOWN`(`session_date` null).
- `roll_state`: `front`·`next`·`expiring`·`expired`. 기준일은 `session_date`, 없으면 KST
  날짜입니다. 세 번째 이후 계약의 봉은 Python 처럼 쓰지 않고 `no_roll_state` 로 셉니다.
- 최종거래일의 휴장 보정 달력은 Python 과 같이 2026-11-23~2026-12-30 만 있고, 그 밖은 둘째
  목요일입니다. Python `session_calendar.py` 를 넓힐 때 `contracts.go` 도 함께 넓힙니다.

### 달력

`calendar_file` 에 auto_trader 의 XKRX 달력에서 옮긴 **평일 휴장일 전부**(연말 휴장일 포함)를
넣습니다. 주말은 자동 휴장입니다. `from`~`to` 밖의 날은 확인할 수 없으므로 `UNKNOWN` 입니다.
빠뜨린 휴장일은 거래일로 처리됩니다.

```json
{"from": "2026-01-01", "to": "2026-12-31", "kr_closed": ["2026-12-25", "2026-12-31"]}
```

파일은 시작할 때 한 번 읽습니다.

### 1분봉 규칙

- `봉 시작 + 1분 + 10초 <= 현재 시각` 일 때만 씁니다. 종료할 때 형성 중인 봉은 버립니다.
- `volume` 은 받은 체결 수량(`last_cnqn`)의 합입니다.
- `volume_gap = true`: 누적 거래량(`acml_vol`)이 이어지지 않거나 세션 중간부터 받아, 그
  봉이나 바로 앞에서 체결이 빠졌을 수 있습니다. 이미 쓴 봉 뒤의 누락은 다음 봉에만
  표시됩니다.
- 누적 거래량이 늘지 않은 반복 프레임과 직전 체결보다 시각이 이른 재전송은 버리고, 이미
  닫힌 분의 늦은 체결도 버립니다(각각 `duplicate`, `late`).
- 체결 시각에는 날짜가 없어 수신 시각에서 정합니다. 수신 시각과 5분 넘게 다르면 버립니다.

### 설정과 출력

```toml
[kis_futures]
enabled = true
ingest = false     # true 는 설정 로드에서 거부(exit 78)
calendar_file = "/etc/fillwire/krx-calendar.json"
jsonl_path = ""    # 비우면 Redis Stream, 값이 있으면 그 파일에만 추가
max_len = 100000   # XADD MAXLEN ~ N
```

`ingest` 는 `false` 만 허용합니다. auto_trader 의 research 분봉 ingest(hk 6343)는 `kr`·`us`
만 받아 `research.kr_futures_candles_1m` 에 쓸 수 없습니다. 선물 ingest(hk 6430)가 머지된 뒤
후속 PR 에서 POST 를 추가합니다.

Redis Stream 키는 `minute:kis:futures:<symbol>`(예: `minute:kis:futures:F200.2612`)이며 JSONL
도 같은 필드입니다: `time_utc`(봉 시작), `symbol`, `product`, `contract_month`(YYMM),
`roll_state`, `session_segment`, `session_date`, `source`(`KIS`), `open`, `high`, `low`,
`close`, `volume`, `received_at`(봉을 닫은 시각), `volume_gap`. Redis 에서는 알 수 없는
`session_date` 필드를 생략하고 JSONL 에서는 `null` 입니다. 컨테이너에서 JSONL 을 쓰려면 쓰기
가능한 볼륨이 필요합니다.

설정이 거부되거나 달력·출력을 열 수 없으면 선물 레인만 꺼지고 체결 경로는 그대로입니다.
출력 쓰기가 실패하면 닫힌 봉을 최대 2000개까지 보관했다가 순서대로 다시 씁니다. 다시 써도
중복되지 않습니다.

- Redis: 한 symbol 의 한 분은 한 번만 추가합니다(먼저 쓴 것이 남음). `XADD` 와 함께
  `dedupe:minute:kis:futures:<symbol>:<time_utc>` 표시 키(TTL 48시간)를 원자적으로 씁니다.
- JSONL: 쓰다 만 줄은 잘라낸 뒤 다시 씁니다. 이 파일에는 이 레인만 써야 합니다.

### 켠 뒤 확인

```sh
sudo journalctl -u fillwire.service -n 200 --no-pager | grep -E 'KIS 선물'
# expected: "KIS 선물 구독 계약" contracts=[F200.… MF200.…]
# investigate: "KIS 선물 구독 실패", "종목코드가 마스터의 계약과 다릅니다", "마스터를 읽지 못했습니다"
redis-cli -u "$REDIS_URL" XREVRANGE minute:kis:futures:F200.2612 + - COUNT 3
# 주간과 야간 각각: 늘어나는 봉, session_segment, session_date, volume_gap
```

첫 smoke 에서 주간 봉 몇 개를 거래소 화면 또는 Python 수집 행과 비교하고, 야간 봉이 실제로
들어오는지 확인합니다. 재접속하면 레인은 모든 선물 스트림을 해지한 뒤 다시 등록합니다.
클라이언트의 복원이 거절돼도 알려 주지 않고, KIS 는 이미 있는 구독의 반복 등록을 거절하기
때문입니다.

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

## KIS mock websocket recorder

`[kis_mock_ws]` is disabled by default. Enable it, set domestic `symbols` and `recording_directory`, and set `[kis].endpoint = "mock"` and `account_mode = "mock"`. Set `app_key_env = "KIS_MOCK_APP_KEY"` and `app_secret_env = "KIS_MOCK_APP_SECRET"` for the auto_trader mock credential names. Run `fillwire kis-mock-ws`; it refuses non-mock settings before dialing. It records domestic `H0STCNT0`/`H0STASP0`, overseas `H0GSCNI9`, and futures/options notices in `internal/recording`. The VTS futures notice TR defaults to `H0IFCNI9`, matching the KIS mock-notice pattern and legacy mock label; this default is unverified until the first mock session. Set `[kis_mock_ws].futures_notice_tr_id = "H0IFCNI0"` to switch to the current official sample's ID. Frames under either ID are decoded and recorded. The session allows at most 41 registrations, matching fillwire's existing conservative setting; the exact KIS limit remains unverified. The command remains available as `fillwire kis-mock-ws` alongside the shared `serve` registry.

`[kis_mock_ws]`는 기본적으로 꺼져 있습니다. 활성화하고 국내 `symbols`, `recording_directory`를 설정한 뒤 `[kis].endpoint = "mock"`, `account_mode = "mock"`로 맞춥니다. auto_trader 모의 환경변수 이름은 `app_key_env = "KIS_MOCK_APP_KEY"`, `app_secret_env = "KIS_MOCK_APP_SECRET"`입니다. `fillwire kis-mock-ws`로 실행하며 연결 전에 모의 설정이 아닌 경우 거부합니다. 국내 `H0STCNT0`/`H0STASP0`, 해외 `H0GSCNI9`, 선물옵션 통보를 `internal/recording`에 기록합니다. VTS 선물옵션 통보 TR 기본값은 KIS 모의 통보 패턴 및 레거시 모의 라벨과 일치하는 `H0IFCNI9`이며, 첫 모의 세션 전까지 검증되지 않았습니다. 현재 공식 샘플의 ID로 전환하려면 `[kis_mock_ws].futures_notice_tr_id = "H0IFCNI0"`로 설정합니다. 두 ID의 프레임을 모두 디코딩하고 기록합니다. 주문 또는 ledger ingest 경로는 사용하지 않습니다. 세션 등록은 fillwire의 기존 보수적 설정인 최대 41건이며, KIS 공식 한도는 확인되지 않았습니다. 공용 `serve` registry와 함께 `fillwire kis-mock-ws` 명령도 계속 사용할 수 있습니다.

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

## 키움 모의 웹소켓 기록기 (기본 비활성화)

`[kiwoom_mock_kr]`와 `[kiwoom_mock_us]`는 서로 다른 모의 계정 reader입니다. 설정한 국내·미국 종목 수의 합은 최대 200개이며, 제한을 넘으면 접속 전에 설정을 거부합니다. 미국 심볼은 Kiwoom 거래소 코드 `ND`(NASDAQ), `NY`(NYSE), `NA`(AMEX)를 각각 지정합니다. 각 reader는 해당 계정 전용 `KIWOOM_MOCK_*` 환경 변수 이름만 설정에서 읽고, 모의 OAuth에서 받은 토큰으로만 `wss://mockapi.kiwoom.com:10000/api/websocket`에 로그인합니다. 라이브 호스트는 dial 전에 거부합니다.

미국 심볼 설정 예시:

```toml
[kiwoom_mock_us]
symbols = [{ code = "NVDA", exchange = "ND" }, { code = "IBM", exchange = "NY" }]
```

```sh
fillwire kiwoom-kr --config /etc/fillwire/fillwire.toml
fillwire kiwoom-us --config /etc/fillwire/fillwire.toml
```

국내 심볼은 `0B`, `0D`, `0H`, `1h`를 구독하고, 미국 심볼은 `FE`, `FT`를 구독합니다. 같은 세션에서 국내 계좌는 `00`, `04`, 미국 계좌는 `F4`, `F5` 계좌 스트림을 받습니다. 받은 프레임은 `<directory>/kiwoom-kr/` 또는 `kiwoom-us/` 아래 UTC 날짜별 JSONL 기록에 `received_at`과 함께 추가합니다. 기록기에는 원장 ingest나 주문 경로가 없습니다.

프로토콜 근거: 키움 공식 [OAuth 접근토큰 발급(au10001)](https://openapi.kiwoom.com/guide/apiGuideContents/a1/au10001), [국내 주식체결(0B)](https://openapi.kiwoom.com/guide/apiGuideContents/14/0B), [미국 10호가(FE)](https://openapi.kiwoom.com/guide/apiGuideContents/14/FE), [미국 실시간 TR 목록(F4/F5/FE/FT 포함)](https://openapi.kiwoom.com/m/guide/apiguide), [웹소켓 연결 및 LOGIN 안내](https://openapi.kiwoom.com/m/guide/index), [주문체결(00)의 공통 등록 형식 및 200종목 한도](https://openapi.kiwoom.com/m/guide/apiguide/14/00).
## Alpaca paper 스트림 기록기 (기본 비활성화)

`[alpaca] enabled = true`와 `symbols` 종목 목록 및 `directory`를 설정하면 `fillwire alpaca --config fillwire.toml`이 Alpaca paper 계정의 `trade_updates`와 IEX의 주식 trade/quote/minute bar 프레임을 UTC 날짜별 JSONL 파일에 기록합니다. 명령은 paper 거래 스트림 `wss://paper-api.alpaca.markets/stream`에만 연결하며 라이브 거래 스트림 호스트는 다이얼 전에 거부합니다. 구독 전에 API 인증을 수행합니다. 주문 제출이나 ledger ingest를 호출하지 않습니다.

키 값은 설정 파일에 넣지 않습니다. 프로세스는 `ALPACA_PAPER_API_KEY`, `ALPACA_PAPER_API_SECRET`, `ALPACA_DATA_API_KEY_ID`, `ALPACA_DATA_API_SECRET_KEY` 환경 변수에서 읽습니다. 기본 IEX feed는 무료 market-data plan에서 제공되는 feed입니다. 연결이 끊기면 재시도하며, 형식이 잘못된 프레임은 버립니다. 기록은 `<directory>/alpaca-trade-updates/<UTC 날짜>/trade_updates.jsonl` 및 `<directory>/alpaca-iex/<UTC 날짜>/market_data.jsonl`에 추가됩니다.

Trading stream의 인증, `trade_updates` 구독 및 fill 이벤트 형식은 [Alpaca WebSocket Streaming 문서](https://docs.alpaca.markets/us/docs/websocket-streaming)를 따릅니다. IEX endpoint와 trades/quotes/bars 구독 형식은 [Alpaca Market Data WebSocket 문서](https://docs.alpaca.markets/us/docs/streaming-market-data) 및 [market data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)를 따릅니다.

## Binance Spot Demo 스트림 기록기 (기본 비활성화)

`[binance_demo] enabled = true`와 `symbols`, `directory`를 설정하면 `fillwire serve`가 Spot Demo 시장 스트림과 서명된 사용자 스트림을 기록합니다. `user_stream`은 기본적으로 켜져 있으며, `false`로 설정하면 시장 스트림만 기록합니다. 사용자 스트림 키는 `BINANCE_SPOT_DEMO_API_KEY`와 `BINANCE_SPOT_DEMO_API_SECRET` 환경 변수에서 읽습니다(각각 `BINANCE_DEMO_API_KEY`와 `BINANCE_DEMO_API_SECRET`을 대체 이름으로 사용할 수 있습니다). 키 값은 설정이나 로그에 기록하지 않습니다. 시장·사용자 프레임은 `received_at`과 함께 JSONL에 추가되며, 거부된 사용자 구독도 기록합니다. 사용자 구독이 거부되어도 시장 스트림은 계속됩니다. 이 모듈은 demo 전용 호스트만 다이얼하고 주문 또는 ledger ingest를 수행하지 않습니다. 단독 실행은 `fillwire binance-demo --symbols BTCUSDT --record-dir /var/lib/fillwire/recordings`입니다.

## 재연결 reconcile 트리거 (기본 비활성화)

`[reconcile] enabled = true`로 켜면 KIS 최초 구독 직후, 재연결 성공 직후, 그리고 `interval`(설정 시)마다 auto_trader의 `reconcile/trigger` 엔드포인트를 호출해 수 초의 체결 통지 공백을 바로 메웁니다. 토큰은 `[ingest]`와 같은 것을 쓰며 로그에 남지 않습니다. 호출은 체결 처리 경로와 분리되어 있어 느리거나 실패해도 처리·종료에 영향이 없습니다. 기본값 `dry_run = true`는 보고만 하며, `dry_run = false`(커밋) 전환과 auto_trader의 30분 TaskIQ 크론 해제는 별도 승인 후의 환경 변경입니다.

## 분봉 shadow 폴러 `market-tap` (기본 비활성화)

`market-tap` 은 KIS 국내(KRX `J`·NTX `NX`)·해외 1분봉 GET 만 호출하는 shadow 폴러입니다. 결과는 JSONL 파일과 Redis Stream(`minute:kis:<venue>:<symbol>`)에만 쓰고 research 표에는 ingest 하지 않습니다. 주문·원장 경로와 KIS 토큰 발급 경로를 링크하지 않으며, 토큰은 Redis 또는 gatewayd `/fetch` 에서 읽기만 합니다. `-enabled` 없이는 아무 요청도 보내지 않습니다. 실행 플래그, 대상 목록·달력 파일 형식, 행 규칙과 검증 방법은 [`docs/market-tap.md`](docs/market-tap.md)를 보세요.

## broker-edge 통합

`broker-edge` 저장소(`mgh3326/broker-edge` @ `0bdec8e`)를 이력째 이 저장소로 들여왔습니다(hk 5872). 명령은 `cmd/{edge-canary,gatewayd,kis-mock-edge,kis-mock-read,market-tap}`, 패키지는 `internal/{canary,gatewayd,kislive,kismockedge,kismockread}`와 `execution_contracts/`에 있고 동작과 플래그는 바뀌지 않았습니다. 배포 전환(hk 5874) 전까지 `Dockerfile.edge`가 `broker-edge`가 게시하던 것과 같은 4개 바이너리 이미지를 만듭니다. 문서는 [`docs/edge/README.md`](docs/edge/README.md)와 `docs/edge/`, `deploy/edge/`, `scripts/edge/`, `fixtures/edge/`로 옮겼습니다. 예약 이름이던 `market-tap` 은 hk 6344에서 분봉 shadow 폴러로 구현되었습니다(위 절 참조).
