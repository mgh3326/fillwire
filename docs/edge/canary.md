# broker-edge canary

`edge-canary` is the standing proof that a machine can still place and cancel a
mock order through its loopback edge. During an eligible regular session it
sends exactly one far-below limit buy, requires `ACCEPTED`, then cancels that
same command and requires `CANCELLED`. It never retries: the per-session
budget is one order, because retrying a possibly accepted order would create a
second-order risk.

It chooses `kis_mock` on weekdays from 09:05 through 15:15 KST and
`kis_mock_us` on weekdays from 09:35 through 15:55 America/New_York time.
The New York location accounts for DST. Outside those windows it exits zero
with `scope=no_session,outcome=no_session`, without placing anything.

For `kis_mock` the canary first asks the edge for the symbol's daily price
band: `GET /v1/price-band?scope=kis_mock&stock_code=<symbol>` is a read-only
inquiry backed by the KIS mock inquire-price read (`FHKST01010100`). The
order price is then derived inside the band: the exchange lower limit
(`stck_llam`, returned as `lower_limit`) verbatim — rounded up to the KRX
tick if the value were ever misaligned — or, when the lower limit is absent,
the band base (`stck_sdpr`, `base_price`) multiplied by 0.70 and rounded UP
to the KRX tick. The result is exactly the band floor (or less than one tick
above it), so the buy sits at the bottom of the daily band: far from the
market and unable to fill in a normal market, while still being a price the
exchange accepts. The order type stays `limit`; nothing about the inquiry
changes the single place-then-cancel budget.

The inquiry fails closed. An unreachable edge, a non-2xx or malformed
response, a mismatched schema or stock code, or a band with neither a usable
lower limit nor a base price yields `outcome=price_unavailable` and no order
is placed. There is deliberately no price knob: `CANARY_KR_PRICE` was removed
because a fixed price can drift outside the daily band. The US path is
unchanged and still sends the constant `AAPL` at `1` without an inquiry.

`CANARY_KR_SYMBOL` defaults to `005930`. `CANARY_EDGE_URL` defaults to
`http://127.0.0.1:8080` and is restricted to loopback. The command ID is
prefixed `broker-edge-canary:` and is also sent as the correlation ID. Each
run writes its result as one JSON object to stdout. Set `CANARY_TEXTFILE_DIR` (default
`/var/lib/node_exporter/textfile`) to expose the atomically replaced
`broker_edge_canary.prom` node_exporter textfile.

## Cancel pacing

Every cancel dispatch waits until at least `1.1 s` has passed since the most
recent edge response, so the place acknowledgement and the cancel are never
back to back. A live run had its cancel refused with `EGW00201`
(`초당 거래건수를 초과하였습니다`, HTTP 500, `rt_cd` 1) only 88 ms after the place
ack. The KIS mock per-second order limit could not be confirmed from a primary
source: community sources disagree (about 2 versus 5 requests per second). The
`1.1 s` value is the fallback that covers a one-request-per-second window. It
applies to both the normal cancel and the post-check cleanup. The wait is not
charged to `durations_ms.cancel_sent`. It is also never cut short by the run
deadline, so an acknowledged order always gets its cancel attempt.

## Carry-over of an unconfirmed cancel

A cancel can end without a confirmed answer while the order still rests on the
broker: a broker answer the edge could not classify (`UNKNOWN`), a throttle
rejection (`REJECTED_THROTTLED`), a cancel that never reached the edge, or a
pre-send failure that wrote no marker. Before any placement, each run asks the
edge for this scope's unconfirmed canary commands in the current trading day:

`GET /v1/cancel-carryover?scope=<kis_mock|kis_mock_us>&placed_from=<RFC3339>&placed_before=<RFC3339>&command_prefix=broker-edge-canary%3A`

The window is the scope's exchange trading day: the Seoul day for `kis_mock`,
the New York day for `kis_mock_us`. The edge lists accepted canary commands on
that scope whose latest cancel is absent, `UNKNOWN`, or `REJECTED_THROTTLED`.
Commands never sent to the broker come first. When the list is empty, the run
continues normally. When it is not empty, the run:

- makes one cancel attempt for the first candidate, through the edge's normal
  `POST /v1/commands/{command_id}/cancel`;
- places nothing in that run;
- exits `1` with `outcome=carry_over_pending`, whatever the cancel answered,
  because a carried-over cancel is not proof that a place works now.

The result carries `carry_over.pending` (the count listed),
`carry_over.command_id`, and `carry_over.cancel_state`. A lookup that fails,
returns a non-200 response, lacks the `candidates` key, or lists a command
outside the canary prefix or the requested scope, or with an unexpected cancel
state, ends with `outcome=carry_over_check_failed`, exits `1`, and places
nothing.

**Release.** A candidate stops blocking the canary at the end of its trading
day, because the window moves on. Day orders are assumed to lapse at the close
of their session. That assumption is not verified against KIS mock behaviour
and is recorded as an open item. Within the day, an `UNKNOWN` candidate blocks
placement until the edge answers differently or the day ends.

## Edge send contract for a cancel

The edge's cancel contract, in `docs/edge/kis-mock-edge.md`, decides whether a
carried-over cancel is sent:

- A cancel attempt is a row in `cancel_attempts` keyed by command and attempt
  number. The row is committed as a pending `UNKNOWN` marker before the send.
- A re-send happens only after the latest attempt is `REJECTED_THROTTLED`. That
  state is recorded only for a KIS mock HTTP 500 with `rt_cd` 1 and `msg_cd`
  `EGW00201`: the broker did not process the request. A re-send waits at least
  1.1 s after the previous attempt.
- At most three cancel attempts per command, the first included.
- Every other answer (any other error, timeout, or unreadable reply) is stored
  `UNKNOWN` and is replayed without a broker call. It is never re-sent.
- Placement is never re-sent.

## Rollout order

The canary and the edge must agree on `GET /v1/cancel-carryover`. Upgrade the
edge before the canary. Roll the canary back before the edge. An edge that lacks
the route answers `404`, so every in-session run ends `carry_over_check_failed`
without placing. The `/metrics` gate does not detect this skew.

## Overall deadline

`Run` creates one overall deadline and passes it to both the carry-over check
and the normal run. The carry-over lookup and its cancel therefore count
against `OverallTimeout`. The normal run's worst chain is band 20 + place 25 +
check 25 + cancel 25 + pacing 1.1, which is 96.1 s and inside the 120 s budget.

## Timeouts

Every stage carries its own deadline and the whole run carries one overall
deadline; the HTTP client itself has no timeout so budgets compose instead of
competing for a single shared timer. Previously one `http.Client{Timeout:15s}`
covered every call and no overall deadline existed: a slow band inquiry could
consume the placement budget and a hung stage could hold the run forever.

| Budget | Old | New | Why |
| ------ | --- | --- | --- |
| band GET (`band_get`) | shared 15s client timeout | 20s | the edge's inquiry is internally capped near 10s (`kismockread.Config.Timeout`); 20s covers loopback plus token overhead without starving the place stage |
| place (`place_sent`) | shared 15s | 25s | the edge's broker send is likewise ~10s-capped; 25s lets the edge's own receipt arrive instead of a local transport timeout |
| resolve check (`check`) | — (did not exist) | 25s | one edge-side evidence read plus a small write margin |
| cancel (`cancel_sent`) | shared 15s | 25s | same broker-side bound as place |
| overall run | none | 120s | one deadline in `Run` covers carry-over and the normal run; worst-case band+place+check+cancel+pacing chain is 96.1s |

## Evidence on every outcome line

Every run — `ok`, `place_not_accepted`, `cancel_not_cancelled`,
`edge_unreachable`, `price_unavailable`, `no_session` — records what it proved:

- `command_id`: this run's unique command ID (safe to print; it is also the
  correlation ID).
- `price`: the limit price actually sent (KR: derived inside the band).
- `band`: the band fields the price was derived from (`stock_code`,
  `lower_limit`, `base_price`), present whenever a band response decoded.
- `stage`: the furthest stage reached — `band_get`, `place_sent`,
  `place_acked`, `cancel_sent`, `cancel_acked`.
- `durations_ms`: wall duration per stage that ran (`band_get`, `place_sent`,
  `check`, `cancel_sent`), measured on a monotonic clock.

When the edge rejects a place or cancel, the object additionally carries
`error_code` (the edge's closed error vocabulary, e.g. `tick_mismatch`,
`broker_5xx`, `token_expired`, or the canary-local `canary_timeout` when a
stage's own deadline elapsed) and, when the broker produced an HTTP response,
`rejection` — an object with the broker's own `rt_cd`, `msg_cd`, `msg1`, and
`http_status`. Each of `rt_cd`, `msg_cd`, `msg1` is emitted only when the
broker's JSON value is a string; any other JSON type is replaced by
`"<non-string omitted>"`. `http_status` is the HTTP status integer from the
response, never a body field. All `rejection` text is masked at capture so
account numbers, access tokens, and application keys can never appear.
`rt_cd` and `msg1` additionally mask generic account-shaped digit runs and
long credential-shaped runs; `msg_cd` is KIS vocabulary rather than a
secret, so only the configured secrets themselves are redacted there and an
ordinary numeric code stays readable. `msg1` is capped at 512 runes.
`rejection` values are response-only evidence: the edge never persists them.

## Ambiguous place results

A place call that times out locally or returns a non-`ACCEPTED`,
non-`NOT_CREATED` receipt (including the edge's own `broker_timeout`) may have
crossed the send boundary, so the canary never assumes the order does not
exist. Before finishing it issues exactly one bounded check —
`POST /v1/commands/{command_id}/resolve` — which makes the edge answer from
its own durable record plus the GET-only daily-order-history read. Match
evidence uses side, stock, quantity, price, and the send-time window. KIS
history can contain an identical foreign order, so even a single candidate
leaves the command `UNKNOWN`.

The check is reported in `order_check` with `disposition`, `evidence_read`,
`orders_seen`, `matched`, and an `error_code` when the check itself could not
answer. Only a `disposition=ACCEPTED` answer dispatches the usual
command-id cancel, whose state lands in `order_check.cancel_state`. No other
order is ever touched, and a successful cleanup does not relabel the failed
place as `ok`: the outcome stays `place_not_accepted` so the timeout remains
alertable. `command_not_found` from the check means the command was absent
from the edge's store when the check ran; it is not proof the edge could
never hold it — the original request may still reserve and send it
afterward, and a check aimed at a different store file than the one that
recorded the command reports the same absence.

The textfile contains `broker_edge_canary_result{scope,outcome}` (only the
last result, value 1), `broker_edge_canary_last_run_timestamp_seconds`, and
`broker_edge_canary_last_success_timestamp_seconds{scope}`. It deliberately
does not put order IDs, symbols, prices, or quantities in labels.

Suggested Grafana/Prometheus alerts:

- During the corresponding session, alert if `time() - broker_edge_canary_last_success_timestamp_seconds{scope=...} > 7200`.
- Alert immediately when `broker_edge_canary_result{outcome!~"ok|no_session"} == 1`.

Install the example systemd service and timer after replacing the image tag and
creating `/etc/broker-edge/canary.env` locally. That file is the place for the
real mock-only edge configuration and must not be committed.
