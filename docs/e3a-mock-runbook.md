# E2 through E1 on the KIS mock account: binding and runbook (ROB-1457)

This document covers the E2 executor (`internal/exec`) sending through the E1 intent gateway (auto_trader ROB-1440) to the KIS mock account. It has two parts:

- The transport binding that the E1 client in `internal/exec/e1` speaks.
- The runbook for the first operator-present mock run.

This change runs nothing. `execmock` can only dry-run, and the mock run below is a later step.

## What this build does

| Config (`[e1]`) | `execmock` | E1 client |
|---|---|---|
| No `account`, or any value except exactly `kis_mock` | Exits 2. Sends nothing | `New` returns `ErrAccountNotMock` |
| `account = "kis_mock"`, `send_enabled` unset or false | Exits 0. Sends nothing | Off: `Send` refuses locally, and the executor's `SendEnabled` is false |
| `account = "kis_mock"`, `-dry-run` | Prints each request as one JSON line. A fake answers it. Sends nothing and reads no token | Dry-run |
| `account = "kis_mock"`, `send_enabled = true` | Exits 2. The live book and ledger feeds are not wired yet | Send: posts to `url` (tested only against an in-process server) |

Recorded dry run:

```sh
go run ./cmd/execmock -config cmd/execmock/testdata/dry-run.toml \
  -intent cmd/execmock/testdata/intent.json \
  -in internal/exec/sim/testdata/sim.evidence.jsonl -dry-run
```

The output is pinned in `cmd/execmock/testdata/dry-run.golden.jsonl`.

## Transport binding

E1 leaves the transport to E2 (`docs/design/1440-intent-gateway-contract.md`, "Transport"). This is the binding the fillwire client sends. The auto_trader route must implement it (see Preconditions).

```http
POST <url>
Content-Type: application/json
X-Execution-Intent-Gateway-Token: <token>
X-Execution-Intent-Confirm: <idempotency_key of this body>

{"action":"submit","intent_id":"<uuid>","idempotency_key":"<uuid>:1","quantity":10,"limit_price_krw":70000}
```

- **URL:** `https`, or `http` to a loopback host only (`localhost`, `127.0.0.0/8` or `::1`), because every request carries the token. A URL with credentials in it is refused. The client never follows a redirect.
- **Body:** exactly one E1 child request: submit, amend or cancel, with E1's field names and no other field. Keys are `<intent_id>:<n>`, which matches E1's key pattern.
- **Auth:** a shared token in the header that `token_header` names. The default is `X-Execution-Intent-Gateway-Token`. The client reads the token only in send mode, and `execmock` never reads one in this build. `New` refuses a `token_header` that is not a valid header name or that names a header the client sets itself (`X-Execution-Intent-Confirm`, `Content-Type`, `Content-Length`, `Host`, `Transfer-Encoding`). It also refuses a token with control characters, non-ASCII bytes, or leading or trailing spaces. Such a config would otherwise fail every request as an unknown outcome.
- **Per-action confirm:** the route passes `confirm=True` to `IntentGateway.handle` only when `X-Execution-Intent-Confirm` equals the body's `idempotency_key`. A missing or different value means `confirm=False`, and E1 answers `confirm_required`.
- **Answer:** HTTP 200 with E1's `GatewayOutcome` as JSON: `status`, `reason`, `child_id`, `broker_order_id`, `result`, `replayed` and `outcome_not_recorded`. The client refuses unknown fields.
- **Unknown outcome:** a non-200 status, a transport error, an unreadable body, or a status the client does not know all count as an unknown outcome. The executor journals it as `uncertain` and never resends that decision.

### How E1 outcomes map to E2

| E1 answer | E2 state |
|---|---|
| `accepted` with `broker_order_id` | The order is tracked. This is not a fill |
| `accepted` with no order id (submit or amend) | `uncertain` |
| `refused` for a submit or amend (every reason) | `Halt = refused`. E2 cancels the working order, if any, then finishes `refused` |
| `refused` for a cancel | `uncertain`. E2 holds for reconcile |
| `uncertain`, `sending`, `conflict`, or an unknown status | `uncertain`. E2 holds for reconcile |

E1 has no reconcile query, so `Query` always fails. An unknown outcome holds the executor until an operator clears it outside the gateway (contract, "Resolving `uncertain` and `sending`").

`internal/exec/e1/contract_test.go` pins every row above against answers that `testdata/record_e1.py` recorded from auto_trader's own `IntentGateway`.

### Caps

E2 mirrors E1's cumulative rule, `decisions.py` `_exposure_by_order` and `_caps_refusal`, in `internal/exec/exposure.go`. Each order lineage counts at its high-water quantity and notional. Every child sent counts, and a cancel frees nothing.

E2 sizes a new order to the room left. If none is left, the intent finishes `e1_caps`. In practice, a second order after an unknown first one finishes the intent, because E1 refuses it anyway. An amend E1 would refuse cancels the order and finishes `e1_caps`. The executor checks every submit and amend against the mirror before it journals the send. Replay does not check, so a journal written before the mirror still opens.

## Preconditions for the mock run

Do not schedule the run until every item holds:

1. **auto_trader route and intent query (separate PR, desk task).** The route implements the binding above: it validates with `parse_child_request`, applies the confirm rule, and calls `IntentGateway.handle`. It is gated by `EXECUTION_INTENT_GATEWAY_ENABLED`, listens on localhost, and checks the token. It also needs an intent query that the fillwire `Query` can call: the working order (by idempotency key, price and open quantity) and the broker filled quantity, from ledger evidence. Until the query exists, any unknown outcome halts the run.
2. **E1 open items** (contract, "Open items for E2 and E3"):
   - The KR mock coordination route is installed, so a submit is not `uncertain` by construction.
   - The follow-up capability for mock amend and cancel is minted, so amend and cancel are not `uncertain` with zero broker calls.
3. **fillwire live feeds:** a live book source for the intent's symbol (the bookrec stream, not a recorded file), and ledger fills mapped back to idempotency keys through the `broker_order_id`s in the journal. Then remove `execmock`'s send-mode refusal in the same PR, which is a blocking path.
4. **Intent:** one intent stored through `SqlAlchemyGatewayStore.store_intent`, with `account = "kis_mock"`. Use the smallest size: 1 share, `max_notional_krw` below 100,000, and expiry within 30 minutes.

## Run steps (operator present)

1. Check that the KIS mock runner control reads `ACTIVE` and that the gateway env gate is exactly `true` on the auto_trader host.
2. Run `execmock -dry-run` against the live book for 1 minute. Check every printed body against the intent's bounds.
3. Set `send_enabled = true` and `account = "kis_mock"`, and start the run, with the journal on local disk.
4. Watch the run until the intent finishes or a stop condition fires.

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
| Any outcome is `uncertain`, `sending` or `conflict` | The order may exist and cannot be reconciled through E1 yet |
| Any `refused` outcome that is not `submissions_killed` or `authorization_expired` | E2 sent something E1 forbids, so the mirror or the binding is wrong |
| A second working order for the intent appears at the broker | Breaks the one-lineage rule |
| A fill without a ledger row, or a ledger row for an unknown order id | Fill attribution is broken |
| `ErrOverfill`, `ErrHalted`, or a journal write error | The executor stopped itself |
| The book goes stale for more than 30 s while an order is working | Pricing has no input. Cancel and stop |

After a stop, cancel the working order through E1. A cancel is allowed under `GLOBAL_FREEZE`. Then attach the journal and the E1 rows to the ticket.
