# KIS live book stream

The optional producer runs inside `fillwire` and shares its existing KIS
websocket session. It adds one domestic `H0STASP0` subscription for the configured
symbol. It never opens a second websocket and places no orders. `bookrec record`
and `bookrec replay` remain offline evidence tools; the live stream uses their
book parser through `internal/bookfeed`.

Enable it in the operator's fillwire configuration, alongside the existing KIS
and Redis settings:

```toml
[books]
enabled = true
symbol = "005930"
access_token_env = "KIS_MOCK_ACCESS_TOKEN"
```

Default: disabled. One symbol per producer instance. Use the same symbol as the
approved execution intent. The existing KIS `app_key_env` and `app_secret_env`
settings name the credential environment variables. The access token is read
from the named environment variable on each REST read; the producer never issues
an OAuth token. The operator owns token freshness and process restarts when its
environment token changes. Do not put credential values in TOML.

With no valid websocket book during the preceding 4 seconds, mock mode attempts
a REST quotation read every 2 seconds:
`GET /uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn`, transaction
`FHKST01010200`, market `J`, symbol `FID_INPUT_ISCD`. The
[KIS quotation example](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_asking_price_exp_ccn/inquire_asking_price_exp_ccn.py)
uses this transaction in both real and demo environments. REST fallback is off
in live mode. A failed subscription/read, missing token, invalid book, or Redis
write failure publishes nothing and logs a fixed warning. The fills reader
continues. A bounded book-event queue can drop snapshots under backpressure;
consumers must check freshness.

Each full snapshot is an `XADD book:kis:<symbol> MAXLEN ~ 10000 *` entry:

| Field | Encoding |
|---|---|
| `symbol` | Six-character domestic issue code |
| `ts_exchange` | RFC 3339 instant derived from the KST exchange clock |
| `ts_received` | RFC 3339 receive instant, including offset |
| `source` | `ws` or `rest` |
| `bid_price_1` … `bid_price_5` | Positive integer KRW strings, best first |
| `bid_qty_1` … `bid_qty_5` | Nonnegative integer share strings |
| `ask_price_1` … `ask_price_5` | Positive integer KRW strings, best first |
| `ask_qty_1` … `ask_qty_5` | Nonnegative integer share strings |

All 24 fields are present. Prices must be ordered on each side and uncrossed.
No level is carried forward from an earlier snapshot. KIS supplies HHMMSS without
a date for these books; `ts_exchange` uses that clock on the nearest KST receive
day, including midnight rollover. This is a date inference, not a broker-supplied
trading date. An old exchange clock remains old; a REST response is never made
fresh merely by restamping its arrival.

`execmock -live-book` reads the newest full snapshot with `XREVRANGE COUNT 1`.
It does not consume or acknowledge the stream. It selects only the intent's
symbol and uses the older of the exchange and receive instants. Future stamps
cannot authorize a send. By default a submit/amend requires age ≤2 seconds;
book silence >30 seconds while an order is working follows cancel-and-stop.
See [the mock runbook](e3a-mock-runbook.md) for send gates and ledger completeness.

The recorded stream fixture in `internal/bookfeed/testdata/stream.jsonl` is a
projection of five order-book frames from the existing
`internal/exec/sim/testdata/sim.evidence.jsonl` at 0, 5, 10, 15 and 20 seconds.
The fixture contains the exact Redis field/value representation. Its live-reader
dry-run test writes those entries to an in-process Redis protocol fixture and
checks every printed E1 request against the intent bounds.
