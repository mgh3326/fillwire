#!/usr/bin/env bash
set -euo pipefail
umask 077

fail() { echo "deploy-serve: $*" >&2; exit 1; }
valid_pin() { [[ $1 =~ ^ghcr\.io/mgh3326/fillwire@sha256:[0-9a-f]{64}$ ]]; }
read_pin() {
  local line trailing
  {
    IFS= read -r line || return 1
    if IFS= read -r trailing || [[ -n $trailing ]]; then return 1; fi
  } < "$1"
  [[ $line == IMAGE=* ]] || return 1
  local pin=${line#IMAGE=}
  valid_pin "$pin" || return 1
  printf '%s\n' "$pin"
}

[[ $# == 2 ]] || fail "usage: $0 {fillwire-serve|quote-collector} {IMAGE@sha256:DIGEST|--rollback|--recover|--check}"
name=$1
case "$name" in
  fillwire-serve|quote-collector) ;;
  *) fail "unknown unit" ;;
esac
state=${FILLWIRE_DEPLOY_STATE_DIR:-/var/lib/fillwire-deploy}
unit=$name.service
current=$state/$unit.image
previous=$current.previous
pending=$current.pending

local_arm64() {
  [[ $(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$1") == linux/arm64 ]]
}

if [[ $2 == --check ]]; then
  pin=$(read_pin "$current") || fail "invalid current pin"
  local_arm64 "$pin" || fail "pinned linux/arm64 image is absent"
  exit
fi
[[ $2 == --rollback || $2 == --recover ]] || valid_pin "$2" || fail "expected a pinned GHCR digest"

mkdir -p "$state"
lock=$state/$unit.lock
mkdir "$lock" || fail "deployment already locked"
trap 'rm -f "$lock/pin"; rmdir "$lock"' EXIT

write_pin() {
  printf 'IMAGE=%s\n' "$2" > "$lock/pin" || return 1
  mv -f "$lock/pin" "$1"
}

restart_and_gate() {
  # Cancel any scheduled auto-restart before clearing the counters.
  systemctl stop "$unit" || return 1
  # A first-start unit may have no loaded state to reset.
  systemctl reset-failed "$unit" || true
  systemctl start "$unit" || return 1
  local attempt status snapshot restarts stable_snapshot=
  local samples=0
  # Allow startup time, then require seven consecutive samples a second apart.
  # The six-second window exceeds the units' five-second RestartSec.
  for ((attempt=0; attempt<16; attempt++)); do
    status=200
    if [[ $name == fillwire-serve ]]; then
      status=$(curl --silent --max-time 2 --output /dev/null --write-out '%{http_code}' \
        http://127.0.0.1:8792/healthz) || status=
    fi
    if [[ $status == 200 ]] && systemctl is-active --quiet "$unit" &&
      snapshot=$(docker container inspect --format '{{.State.Running}} {{.Config.Image}} {{.Id}} {{.State.StartedAt}}' "$name" 2>/dev/null) &&
      [[ $snapshot == "true $1 "?*" "?* ]] &&
      restarts=$(systemctl show --property=NRestarts --value "$unit") &&
      [[ $restarts == 0 ]]; then
      if ((samples == 0)); then
        stable_snapshot=$snapshot
      fi
      [[ $snapshot == "$stable_snapshot" ]] || return 1
      samples=$((samples + 1))
      if ((samples == 7)); then return 0; fi
    elif ((samples > 0)); then
      return 1
    fi
    if ((attempt<15)); then sleep 1; fi
  done
  return 1
}

restore_pending() {
  local restore
  if [[ ! -s $pending ]]; then
    systemctl stop "$unit" || return 1
    rm -f "$current" || return 1
  else
    restore=$(read_pin "$pending") || return 1
    local_arm64 "$restore" || return 1
    write_pin "$current" "$restore" || return 1
    restart_and_gate "$restore" || return 1
  fi
  # Persist the restored pin before clearing the recovery record.
  sync -f "$state" || return 1
  rm -f "$pending"
}

interrupted() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap 'exit 129' HUP
  if [[ -f $pending ]] && ! restore_pending; then
    echo "deploy-serve: recovery failed; pending record retained" >&2
  fi
  exit "$1"
}

if [[ $2 == --recover ]]; then
  [[ -f $pending ]] || fail "no interrupted deployment"
  restore_pending || fail "recovery failed; pending record retained"
  echo "deploy-serve: recovered $unit"
  exit 0
fi
[[ ! -e $pending ]] || fail "interrupted deployment; run --recover first"
# Install the handlers before the recovery record is published. A signal before
# that exits and releases the lock; a signal after it restores the previous pin.
trap 'interrupted 130' INT
trap 'interrupted 143' TERM
trap 'interrupted 129' HUP

old=
if [[ -e $current ]]; then
  old=$(read_pin "$current") || fail "invalid current pin"
fi
pin=$2
if [[ $2 == --rollback ]]; then
  [[ -n $old && -f $previous ]] || fail "no previous deployment"
  pin=$(read_pin "$previous") || fail "invalid previous pin"
fi
docker pull --platform linux/arm64 "$pin" || fail "unknown or unavailable digest"
local_arm64 "$pin" || fail "pinned linux/arm64 image is absent"
if [[ -n $old ]]; then
  local_arm64 "$old" || fail "rollback image is absent"
fi

# Publish the recovery record before changing the boot pin. An empty record
# denotes a first deployment. Rename the record to commit a successful switch.
if [[ -n $old ]]; then
  write_pin "$pending" "$old"
else
  : > "$lock/pin"
  mv -f "$lock/pin" "$pending"
fi
sync -f "$state"
write_pin "$current" "$pin"
if restart_and_gate "$pin"; then
  sync -f "$state"
  if [[ -n $old && $old != "$pin" ]]; then
    mv -f "$pending" "$previous"
  else
    rm -f "$pending"
  fi
  sync -f "$state"
  echo "deploy-serve: $unit $pin"
  exit 0
fi

restore_pending || fail "deployment failed and recovery failed; pending record retained"
[[ -n $old ]] || fail "first deployment failed; unit stopped"
fail "deployment failed; restored $old"
