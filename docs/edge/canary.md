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

A successful run emits exactly `{"scope":...,"outcome":"ok","timestamp":...}`.
When the edge rejects a place or cancel, the object additionally carries
`error_code` (the edge's closed error vocabulary, e.g. `tick_mismatch`,
`broker_5xx`, `token_expired`) and, when the broker produced an HTTP response,
`rejection` — an object with the broker's own `rt_cd`, `msg_cd`, `msg1`, and
`http_status`. Each of `rt_cd`, `msg_cd`, `msg1` is emitted only when the
broker's JSON value is a string; any other JSON type is replaced by
`"<non-string omitted>"`. `http_status` is the HTTP status integer from the
response, never a body field. All `rejection` text is masked at capture so
account numbers, access tokens, and application keys can never appear.
`rt_cd` and `msg1` additionally mask generic account-shaped digit runs and
long credential-shaped runs; `msg_cd` is KIS vocabulary rather than a
secret, so only the configured secrets themselves are redacted there and an
ordinary numeric code stays readable. `msg1` is capped at 512 runes. The
fields are absent on `ok`, `no_session`, `edge_unreachable`, and
`price_unavailable`
outcomes and whenever the edge response could not be decoded. `rejection`
values are response-only evidence: the edge never persists them.

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
