#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
export PATH="$tmp/bin:$PATH"
export FILLWIRE_DEPLOY_STATE_DIR="$tmp/state"
export TEST_LOG="$tmp/log"
export TEST_FAILURE="$tmp/failure"
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
  'container inspect') printf 'true %s\n' "$(cut -d= -f2 "$TEST_CURRENT")" ;;
  *) exit 2 ;;
esac
MOCK
cat > "$tmp/bin/systemctl" <<'MOCK'
#!/bin/bash
set -eu
echo "systemctl $*" >> "$TEST_LOG"
if [[ $1 == restart && -f $TEST_FAILURE ]] && cmp -s "$TEST_FAILURE" "$TEST_CURRENT"; then
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
printf '#!/bin/bash\nexit 0\n' > "$tmp/bin/sleep"
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
! grep -q systemctl "$TEST_LOG"

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
deploy --rollback
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"

printf 'IMAGE=%s\n' "$b" > "$TEST_FAILURE"
: > "$TEST_LOG"
refuse "$b"
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"
[[ $(grep -c '^curl$' "$TEST_LOG") == 1 ]]
grep -q 'deployment failed; restored' "$tmp/output"
rm "$TEST_FAILURE"
export TEST_UNHEALTHY="$tmp/unhealthy"
printf 'IMAGE=%s\n' "$b" > "$TEST_UNHEALTHY"
refuse "$b"
assert_pin "$current" "$a"
assert_pin "$current.previous" "$b"

printf 'IMAGE=%s\nEXTRA=bad' "$a" > "$current"
refuse --check
export TEST_CURRENT="$FILLWIRE_DEPLOY_STATE_DIR/quote-collector.service.image"
: > "$TEST_LOG"
bash scripts/deploy-serve.sh quote-collector "$a"
assert_pin "$TEST_CURRENT" "$a"
! grep -qx curl "$TEST_LOG"

echo 'deploy-serve pin, first failure, collector and rollback tests: PASS'
