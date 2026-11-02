# E2 through E1 on the KIS mock account: binding and runbook (ROB-1457)

This document covers the E2 executor (`internal/exec`) sending through the E1 intent gateway (auto_trader ROB-1440) to the KIS mock account. It has two parts:

- The transport binding that the E1 client in `internal/exec/e1` speaks.
- The runbook for the first operator-present mock run.

This build starts nothing automatically. Sending remains off by default. The mock run requires an operator and the preconditions below.

## What this build does

| Config (`[e1]`) | `execmock` | E1 client |
|---|---|---|
| No `account`, or any value except exactly `kis_mock` | Exits 2. Sends nothing | `New` returns `ErrAccountNotMock` |
| `account = "kis_mock"`, `send_enabled` unset or false | Exits 0. Sends nothing | Off: `Send` refuses locally, and the executor's `SendEnabled` is false |
| `account = "kis_mock"`, `-dry-run` | Prints each request as one JSON line. A fake answers it. Sends nothing and reads no token | Dry-run |
| `account = "kis_mock"`, `send_enabled = true`, without `-send` | Exits 2; sends nothing | Explicit command authorization missing |
| `account = "kis_mock"`, `send_enabled = true`, `-send -live-book -journal FILE` | Works the intent using the Redis book, committed ledger snapshots and mock broker readback | Send: posts to the configured `/execution-intents/children` URL |

Recorded dry run:

```sh
go run ./cmd/execmock -config cmd/execmock/testdata/dry-run.toml \
  -intent cmd/execmock/testdata/intent.json \
  -in internal/exec/sim/testdata/sim.evidence.jsonl -dry-run
```

The output is pinned in `cmd/execmock/testdata/dry-run.golden.jsonl`.

## Transport binding

The merged auto_trader route implements this binding. Set `url` to its `/execution-intents/children` endpoint. E2 derives the read endpoints from that same origin and prefix.

```http
POST <url>
Content-Type: application/json
X-Execution-Intent-Gateway-Token: <token>
X-Execution-Intent-Confirm: <idempotency_key of this body>

{"action":"submit","intent_id":"<uuid>","idempotency_key":"<uuid>:1","quantity":10,"limit_price_krw":70000}
```

- **URL:** `https`, or `http` to a loopback host only (`localhost`, `127.0.0.0/8` or `::1`), because every request carries the token. A URL with credentials in it, or with a query string or fragment, is refused, so a configured URL is safe to log. Transport errors in the journal carry the cause, not the URL. The client never follows a redirect.
- **Body:** exactly one E1 child request: submit, amend or cancel, with E1's field names and no other field. Keys are `<intent_id>:<n>`, which matches E1's key pattern.
- **Auth:** a shared token in the header that `token_header` names. The default is `X-Execution-Intent-Gateway-Token`. The client reads the token only in send mode, and `execmock -dry-run` never reads a gateway or broker token. `New` refuses a `token_header` that is not a valid header name or that names a header the client sets itself (`X-Execution-Intent-Confirm`, `Content-Type`, `Content-Length`, `Host`, `Transfer-Encoding`). It also refuses a token with control characters, non-ASCII bytes, or leading or trailing spaces. Such a config would otherwise fail every request as an unknown outcome.
- **Per-action confirm:** the route passes `confirm=True` to `IntentGateway.handle` only when `X-Execution-Intent-Confirm` equals the body's `idempotency_key`. A missing or different value means `confirm=False`, and E1 answers `confirm_required`.
- **Answer:** HTTP 200 with E1's `GatewayOutcome` as JSON: `status`, `reason`, `child_id`, `broker_order_id`, `result`, `replayed` and `outcome_not_recorded`. The client refuses unknown fields.
- **Unknown outcome:** a non-200 status, a transport error, an unreadable body, or a status the client does not know all count as an unknown outcome. The executor journals it as `uncertain` and never resends that decision. The live command stops for operator reconcile.
- **Read endpoints:** `GET <prefix>/intents/<intent_id>` and `GET <prefix>/intents/<intent_id>/fills`, with the same token header and no confirm header. The client requires a reconciled, non-null query view. Fills use the full snapshot response documented in auto_trader PR #2273, matched at `ac5e8676f04f4f5bcdb3f6d2c978a55bc60afbbd`.

### How E1 outcomes map to E2

| E1 answer | E2 state |
|---|---|
| `accepted` with `broker_order_id` | The order is tracked. This is not a fill |
| `accepted` with no order id (submit or amend) | `uncertain` |
| `refused` for a submit or amend (every reason) | `Halt = refused`. E2 cancels the working order, if any, then finishes `refused` |
| `refused` for a cancel | `uncertain`. E2 holds for reconcile |
| `uncertain`, `sending`, `conflict`, or an unknown status | `uncertain`. E2 holds for reconcile |

`Query` now reads the route's intent endpoint; an unresolved or missing view returns an error. The live command stops on an unknown outcome for operator reconciliation and never treats that view as fill evidence.

`internal/exec/e1/contract_test.go` pins every row above against answers that `testdata/record_e1.py` recorded from auto_trader's own `IntentGateway`.

### Caps

E2 mirrors E1's cumulative rule, `decisions.py` `_exposure_by_order` and `_caps_refusal`, in `internal/exec/exposure.go`. Each order lineage counts at its high-water quantity and notional. Every child sent counts, and a cancel frees nothing.

E2 sizes a new order to the room left. If none is left, the intent finishes `e1_caps`. In practice, a second order after an unknown first one finishes the intent, because E1 refuses it anyway. An amend E1 would refuse cancels the order and finishes `e1_caps`. The executor checks every submit and amend against the mirror before it journals the send. Replay does not check, so a journal written before the mirror still opens.

## Preconditions for the mock run

Do not schedule the run until every item holds:

1. **auto_trader route and reads:** the child and intent query route is merged.
   The committed `/fills` route from PR #2273 must also be deployed, with
   `EXECUTION_INTENT_GATEWAY_ROUTE_ENABLED=true`, a configured token, and
   `EXECUTION_INTENT_GATEWAY_ENABLED=true`. Check all three endpoints before
   scheduling. A missing or incompatible read stops the executor.
2. **E1 open items** (contract, "Open items for E2 and E3"):
   - The KR mock coordination route is installed, so a submit is not `uncertain` by construction.
   - The follow-up capability for mock amend and cancel is minted, so amend and cancel are not `uncertain` with zero broker calls.
3. **fillwire live feeds — wired in this build:** enable the producer for the
   intent's symbol as described in [book-feed.md](book-feed.md). It shares the
   fills websocket for `H0STASP0` and falls back to a 2-second REST asking-price
   poll in mock when the websocket has no fresh book. Verify the stream is
   receiving fresh snapshots before starting the executor.

   `execmock -live-book` reads `book:kis:<symbol>`. It uses the older of exchange
   and receive timestamps. New submits/amends require a book within 2 s by
   default. An order working through more than 30 s of book silence cancels
   and stops as `stale_book`; after restart with no book, the stop timer uses
   the journal's last order move. Unsynced books cannot postpone that stop.

   The live command polls committed effective ledger rows through E1, then
   binds their broker IDs to accepted submit/amend outcomes in its local
   journal. Matching normalizes leading zeros and includes the KST trading day.
   Unknown, missing or ambiguous orders, wrong-side fills, conflicting fill IDs,
   fractional KRW prices, and changed/removed previously counted rows stop for
   operator reconcile. Full snapshots are deduplicated; an upsert or supersession
   is never added on top of already counted evidence. `fills:kis` is not evidence.

   A clean finish additionally requires every journaled order's ledger
   `cumulative_qty` to equal readback `tot_ccld_qty` for the exact ODNO/day,
   symbol and side, with terminal broker evidence: full fill, `cncl_yn=Y`, or
   `rjct_qty=ord_qty`. Ambiguous, missing or paginated readback stops. An open
   order with matching quantities can keep working; it cannot justify a clean
   finish. Cancel acceptance alone is not terminal ledger completeness. Broker
   readback uses the mock-only `VTTC0081R` GET transaction, bounded to one
   request per second per client. Polls run at a 1-second cadence. Confirmed
   terminal exact-order readbacks are cached for that run, while ledger rows
   are refreshed and compared on every poll; working and terminal-unverified
   orders are reread. A poll has a 5-second overall deadline. A poll deadline
   engages the kill switch, attempts cancellation and stops for reconcile.
4. **Intent:** one intent stored through `SqlAlchemyGatewayStore.store_intent`, with `account = "kis_mock"`. Use the smallest size: 1 share, `max_notional_krw` below 100,000, and expiry within 30 minutes.

## Run steps (operator present)

1. Check that the KIS mock runner control reads `ACTIVE`, the route and gateway
   gates are exactly `true`, and the `/fills` route is available. Confirm the
   producer's symbol equals the approved intent's symbol.
2. Configure the command below. Fill in the operator URL and supply credential
   values through the named environment variables. Keep that local configuration
   outside version control.

   ```toml
   [e1]
   account = "kis_mock"
   send_enabled = false
   url = "" # replace with the operator's /execution-intents/children URL
   token_env = "EXECUTION_INTENT_GATEWAY_TOKEN"
   timeout = "2s"

   [redis]
   url_env = "REDIS_URL"

   [broker_read]
   app_key_env = "KIS_MOCK_APP_KEY"
   app_secret_env = "KIS_MOCK_APP_SECRET"
   access_token_env = "KIS_MOCK_ACCESS_TOKEN"
   account_env = "KIS_MOCK_ACCOUNT"
   product_env = "KIS_MOCK_PRODUCT"
   ```

3. Run `execmock -config FILE -intent INTENT -live-book -dry-run -duration 1m`.
   It prints each prospective body through the E1 client and uses a fake gateway;
   it reads no gateway or broker token. Check all bodies against the intent's
   quantity, notional, price and inventory bounds. The recorded Redis-stream
   fixture is `internal/bookfeed/testdata/stream.jsonl` and is exercised by
   `TestLiveBookDryRunRecordedStreamBodiesWithinBounds`.
4. Set `send_enabled = true` and run
   `execmock -config FILE -intent INTENT -live-book -send -journal FILE`.
   All three gates are required: mock account, enabled config, explicit flag.
   Recorded `-in` evidence cannot send. Keep the journal on durable local disk.
5. Watch until the intent finishes or a stop condition fires. On interrupt, the
   command attempts a cancel and exits for operator reconcile; it does not claim
   terminal completeness. Inspect broker and ledger before restarting.

## What to watch

- **Journal** (`<journal>`, one line per transition):
  - every `send` is followed by an `outcome`;
  - no key repeats;
  - an `outcome` of `uncertain` is followed only by `reconcile` attempts.
- **E1 rows** (`review.execution_child_requests`): one row per key, and every state matches the journal's outcome.
- **Ledger:** every fill the executor counts has a ledger row and a `broker_order_id` from the journal.
- **Broker (KIS mock HTS):** at most one working order for the intent, and its price is never beyond the intent's cap.

## Stop conditions

Stop, then set `GLOBAL_FREEZE` or stop the process, on any of these:

| Condition | Why |
|---|---|
| Any outcome is `uncertain`, `sending` or `conflict` | The order may exist; stop until the query and exact broker readback resolve it |
| Any `refused` outcome that is not `submissions_killed` or `authorization_expired` | E2 sent something E1 forbids, so the mirror or the binding is wrong |
| A second working order for the intent appears at the broker | Breaks the one-lineage rule |
| A fill without a ledger row, or a ledger row for an unknown order id | Fill attribution is broken |
| `ErrOverfill`, `ErrAttribution`, `ErrIncomplete`, `ErrHalted`, or a journal write error | The executor stopped itself |
| The book goes stale for more than 30 s while an order is working | Pricing has no input. Cancel and stop |

After a stop, reconcile the exact broker order and committed ledger rows. Cancel the working order through E1. A cancel is allowed under `GLOBAL_FREEZE`. Then attach the journal and the E1 rows to the ticket.
