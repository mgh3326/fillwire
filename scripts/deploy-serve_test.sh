#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
export TEST_REAL_MV=$(command -v mv)
export PATH="$tmp/bin:$PATH"
export FILLWIRE_DEPLOY_STATE_DIR="$tmp/state"
export TEST_LOG="$tmp/log"
export TEST_FAILURE="$tmp/failure"
export TEST_FLAP="$tmp/flap"
export TEST_TICK="$tmp/tick"
export TEST_RESTARTS="$tmp/restarts"
export TEST_STARTS="$tmp/starts"
export TEST_STOPPED="$tmp/stopped"
export TEST_INTERRUPT="$tmp/interrupt"
export TEST_PUBLISH_INTERRUPT="$tmp/publish-interrupt"
touch "$TEST_TICK" "$TEST_RESTARTS" "$TEST_STARTS"
a=ghcr.io/mgh3326/fillwire@sha256:$(printf '%064d' 1)
b=ghcr.io/mgh3326/fillwire@sha256:$(printf '%064d' 2)
unknown=ghcr.io/mgh3326/fillwire@sha256:$(printf '%064d' 3)
export UNKNOWN_PIN=$unknown
current=$FILLWIRE_DEPLOY_STATE_DIR/fillwire-serve.service.image
export TEST_CURRENT=$current
cat > "$tmp/bin/docker" <<'MOCK'
#!/bin/bash
set -eu
echo "docker $*" >> "$TEST_LOG"
case "$1 $2" in
  'pull --platform') [[ $3 == linux/arm64 && $4 != "$UNKNOWN_PIN" ]] ;;
  'image inspect') [[ $5 != "$UNKNOWN_PIN" ]] && echo linux/arm64 ;;
  'container inspect')
    running=true
    if [[ -f $TEST_FLAP && -s $TEST_TICK ]] && cmp -s "$TEST_FLAP" "$TEST_CURRENT"; then
      running=false
    fi
    pin=$(cut -d= -f2 "$TEST_CURRENT")
    id=container-id
    if [[ -f ${TEST_REPLACE:-} && -s $TEST_TICK ]] && cmp -s "$TEST_REPLACE" "$TEST_CURRENT"; then
      id=replacement-id
    fi
    if [[ $4 == *'.Id'* ]]; then
      printf '%s %s %s 2026-11-26T00:00:00Z\n' "$running" "$pin" "$id"
    else
      printf '%s %s\n' "$running" "$pin"
    fi
    ;;
  *) exit 2 ;;
esac
MOCK
cat > "$tmp/bin/systemctl" <<'MOCK'
#!/bin/bash
set -eu
echo "systemctl $*" >> "$TEST_LOG"
case "$1" in
  stop) touch "$TEST_STOPPED" ;;
  reset-failed)
    [[ -f $TEST_STOPPED ]] || exit 1
    : > "$TEST_STARTS"
    : > "$TEST_RESTARTS"
    ;;
  start|restart)
    rm -f "$TEST_STOPPED"
    : > "$TEST_TICK"
    [[ $(wc -l < "$TEST_STARTS") -lt 4 ]] || exit 1
    echo start >> "$TEST_STARTS"
    if [[ -f $TEST_INTERRUPT ]]; then
      signal=$(cat "$TEST_INTERRUPT")
      rm "$TEST_INTERRUPT"
      kill -s "$signal" "$PPID"
      exit 1
    fi
    ;;
  show)
    if [[ -f ${TEST_AUTO_RESTART:-} && -s $TEST_TICK ]] && cmp -s "$TEST_AUTO_RESTART" "$TEST_CURRENT"; then
      echo restart > "$TEST_RESTARTS"
      # Automatic starts consume the start limit, too.
      printf 'start\nstart\nstart\n' >> "$TEST_STARTS"
    fi
    wc -l < "$TEST_RESTARTS" | tr -d ' '
    ;;
esac
if [[ ( $1 == start || $1 == restart ) && -f $TEST_FAILURE ]] && cmp -s "$TEST_FAILURE" "$TEST_CURRENT"; then
  exit 1
fi
MOCK
cat > "$tmp/bin/curl" <<'MOCK'
#!/bin/bash
echo curl >> "$TEST_LOG"
if [[ -f ${TEST_UNHEALTHY:-} ]] && cmp -s "$TEST_UNHEALTHY" "$TEST_CURRENT"; then
  printf 503
else
  printf 200
fi
MOCK
printf '#!/bin/bash\necho tick >> "$TEST_TICK"\n' > "$tmp/bin/sleep"
printf '#!/bin/bash\n[[ $1 == -f && -d $2 ]]\n' > "$tmp/bin/sync"
# After the recovery record is published, the mock signals the deploy process
# once, so the handlers must restore the pin before the candidate is switched.
cat > "$tmp/bin/mv" <<'MOCK'
#!/bin/bash
"$TEST_REAL_MV" "$@" || exit
if [[ ${@: -1} == *.pending && -f $TEST_PUBLISH_INTERRUPT ]]; then
  signal=$(cat "$TEST_PUBLISH_INTERRUPT")
  rm "$TEST_PUBLISH_INTERRUPT"
  kill -s "$signal" "$PPID"
fi
MOCK
chmod +x "$tmp/bin/"*
deploy() { bash scripts/deploy-serve.sh fillwire-serve "$1"; }
refuse() {
  if deploy "$1" > "$tmp/output" 2>&1; then
    echo "expected deploy to reject $1" >&2
    exit 1
  fi
}
assert_pin() { [[ $(cat "$1") == "IMAGE=$2" ]]; }

refuse ghcr.io/mgh3326/fillwire:main
[[ ! -e $TEST_LOG ]]
refuse "$unknown"
[[ ! -e $current ]]
if grep -q systemctl "$TEST_LOG"; then exit 1; fi

printf 'IMAGE=%s\n' "$a" > "$TEST_FAILURE"
refuse "$a"
[[ ! -e $current ]]
grep -qx 'systemctl stop fillwire-serve.service' "$TEST_LOG"
rm "$TEST_FAILURE"

deploy "$a"
deploy --check
deploy "$b"
assert_pin "$current" "$b"
assert_pin "$current.previous" "$a"
printf 'start\nstart\nstart\nstart\n' > "$TEST_STARTS"
printf 'restart\n' > "$TEST_RESTARTS"
deploy --rollback || { echo 'manual rollback must clear both systemd counters' >&2; exit 1; }
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"

printf 'IMAGE=%s\n' "$b" > "$TEST_FAILURE"
: > "$TEST_LOG"
refuse "$b"
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"
grep -qx curl "$TEST_LOG"
grep -q 'deployment failed; restored' "$tmp/output"
rm "$TEST_FAILURE"
export TEST_UNHEALTHY="$tmp/unhealthy"
printf 'IMAGE=%s\n' "$b" > "$TEST_UNHEALTHY"
refuse "$b"
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"

unset TEST_UNHEALTHY

for fault in TEST_REPLACE TEST_AUTO_RESTART; do
  export "$fault=$tmp/fault"
  printf 'IMAGE=%s\n' "$b" > "$tmp/fault"
  refuse "$b"
  grep -q 'deployment failed; restored' "$tmp/output"
  assert_pin "$current" "$a"
  assert_pin "$current.previous" "$b"
  unset "$fault"
done

# The mock interrupts the deploy process immediately after the candidate pin changes.
for signal in INT TERM HUP; do
  echo "$signal" > "$TEST_INTERRUPT"
  refuse "$b"
  assert_pin "$current" "$a"
  assert_pin "$current.previous" "$b"
  [[ ! -e $current.pending ]]
done

echo KILL > "$TEST_INTERRUPT"
refuse "$b"
assert_pin "$current" "$b"
assert_pin "$current.pending" "$a"
rmdir "$FILLWIRE_DEPLOY_STATE_DIR/fillwire-serve.service.lock"
refuse "$b"
grep -q 'interrupted deployment' "$tmp/output"
printf 'IMAGE=%s\n' "$a" > "$TEST_FAILURE"
refuse --recover
assert_pin "$current.pending" "$a"
rm "$TEST_FAILURE"
deploy --recover
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"
[[ ! -e $current.pending ]]

rm "$current" "$current.previous"
echo TERM > "$TEST_INTERRUPT"
refuse "$b"
[[ ! -e $current && ! -e $current.previous && ! -e $current.pending ]]
echo KILL > "$TEST_INTERRUPT"
refuse "$b"
rmdir "$FILLWIRE_DEPLOY_STATE_DIR/fillwire-serve.service.lock"
deploy --recover
[[ ! -e $current && ! -e $current.previous && ! -e $current.pending ]]

# A signal that lands after the recovery record is published must restore the
# previous pin and release the lock. The first-deployment case has no old pin.
deploy "$a"
for signal in INT TERM HUP; do
  echo "$signal" > "$TEST_PUBLISH_INTERRUPT"
  refuse "$b"
  assert_pin "$current" "$a"
  [[ ! -e $current.pending && ! -e $current.previous ]]
  [[ ! -e $FILLWIRE_DEPLOY_STATE_DIR/fillwire-serve.service.lock ]]
done
rm "$current"
echo TERM > "$TEST_PUBLISH_INTERRUPT"
refuse "$b"
[[ ! -e $current && ! -e $current.pending && ! -e $current.previous ]]
[[ ! -e $FILLWIRE_DEPLOY_STATE_DIR/fillwire-serve.service.lock ]]

printf 'IMAGE=%s\nEXTRA=bad' "$a" > "$current"
refuse --check
export TEST_CURRENT="$FILLWIRE_DEPLOY_STATE_DIR/quote-collector.service.image"
: > "$TEST_LOG"
bash scripts/deploy-serve.sh quote-collector "$a"
assert_pin "$TEST_CURRENT" "$a"
if grep -qx curl "$TEST_LOG"; then exit 1; fi

printf 'IMAGE=%s\n' "$unknown" > "$TEST_CURRENT.previous"
printf 'IMAGE=%s\n' "$b" > "$TEST_FLAP"
if bash scripts/deploy-serve.sh quote-collector "$b" > "$tmp/output" 2>&1; then
  echo 'expected a collector that exits after the first sample to roll back' >&2
  exit 1
fi
grep -q 'deployment failed; restored' "$tmp/output"
assert_pin "$TEST_CURRENT" "$a"
assert_pin "$TEST_CURRENT.previous" "$unknown"

echo 'deploy-serve pin, start limits, stability, signal recovery and rollback tests: PASS'
