#!/bin/sh
# Self-contained tests for deploy-pull.sh. No Docker daemon, systemd, or HTTP
# endpoint is contacted: each is replaced by a command at the front of PATH.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
script=$root/scripts/deploy-pull.sh
tmp=$(mktemp -d "${TMPDIR:-/tmp}/deploy-pull-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
bin=$tmp/bin
mkdir -p "$bin"

cat > "$bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$TEST_LOG/docker"
case "$1" in
  pull) exit 0 ;;
  image)
    printf '%s\n' 'ghcr.io/mgh3326/broker-edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ;;
esac
EOF
cat > "$bin/systemctl" <<'EOF'
#!/bin/sh
set -eu
printf '%s %s ' "$1" "$2" >> "$TEST_LOG/systemctl"
cat "$BROKER_EDGE_STATE_DIR/$2.image" >> "$TEST_LOG/systemctl"
EOF
cat > "$bin/curl" <<'EOF'
#!/bin/sh
set -eu
count_file=$TEST_LOG/curl-count
count=0
[ -f "$count_file" ] && count=$(cat "$count_file")
count=$((count + 1))
printf '%s\n' "$count" > "$count_file"
[ "$count" -le "${CURL_FAIL_COUNT:-0}" ] && exit 22
printf '200'
EOF
chmod +x "$bin/docker" "$bin/systemctl" "$bin/curl"

digest_a=ghcr.io/mgh3326/broker-edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
digest_b=ghcr.io/mgh3326/broker-edge@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
digest_c=ghcr.io/mgh3326/broker-edge@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc

new_case() {
  case_dir=$tmp/$1
  mkdir -p "$case_dir/state" "$case_dir/log"
  TEST_LOG=$case_dir/log
  export TEST_LOG
  BROKER_EDGE_STATE_DIR=$case_dir/state
  export BROKER_EDGE_STATE_DIR
  PATH=$bin:$PATH
  export PATH
  BROKER_EDGE_HEALTH_ATTEMPTS=1
  BROKER_EDGE_HEALTH_INTERVAL=0
  export BROKER_EDGE_HEALTH_ATTEMPTS BROKER_EDGE_HEALTH_INTERVAL
  unset CURL_FAIL_COUNT
}

new_case success
printf 'IMAGE=%s\n' "$digest_b" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image"
"$script" broker-edge.service main
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image")" = "IMAGE=$digest_a" ]
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image.previous")" = "IMAGE=$digest_b" ]
grep -F "restart broker-edge.service IMAGE=$digest_a" "$TEST_LOG/systemctl" >/dev/null

new_case failed-gate
printf 'IMAGE=%s\n' "$digest_b" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image"
printf 'IMAGE=%s\n' "$digest_c" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image.previous"
CURL_FAIL_COUNT=1
export CURL_FAIL_COUNT
if "$script" broker-edge.service main >"$TEST_LOG/output" 2>&1; then
  echo "expected failed health gate to return failure" >&2
  exit 1
fi
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image")" = "IMAGE=$digest_b" ]
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image.previous")" = "IMAGE=$digest_c" ]
grep -F "restart broker-edge.service IMAGE=$digest_b" "$TEST_LOG/systemctl" >/dev/null
grep -F 'health gate failed; restored' "$TEST_LOG/output" >/dev/null

new_case no-previous
printf 'IMAGE=%s\n' "$digest_b" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image"
if "$script" --rollback broker-edge.service >"$TEST_LOG/output" 2>&1; then
  echo "expected rollback with no previous image to return failure" >&2
  exit 1
fi
grep -F 'no valid previous image for broker-edge.service' "$TEST_LOG/output" >/dev/null
[ ! -e "$TEST_LOG/systemctl" ]

new_case rollback
printf 'IMAGE=%s\n' "$digest_a" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image"
printf 'IMAGE=%s\n' "$digest_b" > "$BROKER_EDGE_STATE_DIR/broker-edge.service.image.previous"
"$script" --rollback broker-edge.service
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image")" = "IMAGE=$digest_b" ]
[ "$(cat "$BROKER_EDGE_STATE_DIR/broker-edge.service.image.previous")" = "IMAGE=$digest_a" ]
grep -F "restart broker-edge.service IMAGE=$digest_b" "$TEST_LOG/systemctl" >/dev/null

echo "deploy-pull tests: PASS"
