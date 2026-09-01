# kis-mock-edge boundary design

## Current state

`kis-mock-edge` is intentionally only a reserved command name. Its directory
contains documentation (`cmd/kis-mock-edge/doc.go`) and no `main` function,
listener, panic stub, HTTP route, or broker client. Starting a server is not
part of this change.

Python remains the owner of `order_send_intents`. That flow is unchanged and is
not redirected through Go by this design document.

## Proposed future boundary

If separately approved, Go may receive an `ExecutionCommand` at a narrow edge
boundary. `execution_contracts.ExecutionCommandV1` is only a placeholder name
for that future command shape; it is not a transport, queue, HTTP API, or
authorization decision today.

The future receiver must be introduced as an explicit design and implementation
change. It must specify authentication, idempotency, authorization, audit
records, failure behavior, and the exact mutation policy before any HTTP server
or broker operation is added.

## Hard limits for the reserved command

- Mutation count is zero: no order placement, modification, cancellation, or
  other broker POST operation.
- Token issuance count is zero: it must not issue, refresh, clear, migrate, or
  write token material.
- No live host, credentials, tokens, or response payloads belong in this
  command or document.
- `kis-mock-read` remains the independent, GET-only VTS read path; this
  reserved command does not proxy or widen its allowlist.

Until a separate approval lands, the correct implementation of
`cmd/kis-mock-edge` is the documentation-only directory already present in the
repository.
