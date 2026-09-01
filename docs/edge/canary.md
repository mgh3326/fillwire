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

Domestic defaults are `CANARY_KR_SYMBOL=005930` and `CANARY_KR_PRICE=1000`;
US defaults are `AAPL` at `1`. `CANARY_EDGE_URL` defaults to
`http://127.0.0.1:8080` and is restricted to loopback. The command ID is
prefixed `broker-edge-canary:` and is also sent as the correlation ID. Each
run writes its result as one JSON object to stdout. Set `CANARY_TEXTFILE_DIR` (default
`/var/lib/node_exporter/textfile`) to expose the atomically replaced
`broker_edge_canary.prom` node_exporter textfile.

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
