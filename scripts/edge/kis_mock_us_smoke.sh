#!/usr/bin/env sh
set -eu

# This is deliberately a local-client smoke only. The edge itself remains the
# sole holder of the VTS mutation capability and must already be explicitly
# enabled by its operator.
edge_url=${BROKER_EDGE_SMOKE_URL:-http://127.0.0.1:8080}
limit_price=${BROKER_EDGE_SMOKE_AAPL_LIMIT_PRICE:-1}

case "$edge_url" in
  http://127.0.0.1:*) ;;
  *)
    printf '%s\n' 'kis_mock_us smoke: BROKER_EDGE_SMOKE_URL must use 127.0.0.1' >&2
    exit 2
    ;;
esac

case "$limit_price" in
  [1-9]|[1-9][0-9]*) ;;
  *)
    printf '%s\n' 'kis_mock_us smoke: limit price must be a positive integer' >&2
    exit 2
    ;;
esac
if [ "$limit_price" -gt 50 ]; then
  printf '%s\n' 'kis_mock_us smoke: limit price must be at most 50' >&2
  exit 2
fi

command_id="broker-edge-smoke:$(date -u +%Y%m%dT%H%M%SZ):$$"
command_payload=$(printf '{"schema_version":"execution-command/v1","command_id":"%s","account_scope":"kis_mock_us","side":"buy","stock_code":"AAPL","quantity":"1","price":"%s","order_type":"limit","issued_at":"%s"}' "$command_id" "$limit_price" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")

place_response=$(curl --fail --silent --show-error --connect-timeout 3 --max-time 15 \
  -H 'content-type: application/json' \
  --data "$command_payload" \
  "$edge_url/v1/commands")
case "$place_response" in
  *'"disposition":"ACCEPTED"'*'"broker_order_id":"'*) ;;
  *)
    printf '%s\n' 'kis_mock_us smoke: placement was not ACCEPTED; cancellation was not attempted' >&2
    exit 1
    ;;
esac

cancel_response=$(curl --fail --silent --show-error --connect-timeout 3 --max-time 15 \
  -X POST \
  "$edge_url/v1/commands/$command_id/cancel")
case "$cancel_response" in
  *'"state":"CANCELLED"'*) ;;
  *)
    printf '%s\n' 'kis_mock_us smoke: cancellation was not CANCELLED' >&2
    exit 1
    ;;
esac

# An ACCEPTED smoke placement has no UNKNOWN receipt to resolve. This bounded
# invocation confirms that the resolver can inspect the same mock-US evidence
# configuration without ever retransmitting the accepted placement or cancel.
go run ./cmd/kis-mock-edge resolve
printf '%s\n' 'kis_mock_us smoke: place ACCEPTED, cancel CANCELLED, resolver completed'
