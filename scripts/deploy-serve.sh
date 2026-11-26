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

[[ $# == 2 ]] || fail "usage: $0 {fillwire-serve|quote-collector} {IMAGE@sha256:DIGEST|--rollback|--check}"
name=$1
case "$name" in
  fillwire-serve|quote-collector) ;;
  *) fail "unknown unit" ;;
esac
state=${FILLWIRE_DEPLOY_STATE_DIR:-/var/lib/fillwire-deploy}
unit=$name.service
current=$state/$unit.image
previous=$current.previous

local_arm64() {
  [[ $(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$1") == linux/arm64 ]]
}

if [[ $2 == --check ]]; then
  pin=$(read_pin "$current") || fail "invalid current pin"
  local_arm64 "$pin" || fail "pinned linux/arm64 image is absent"
  exit
fi
[[ $2 == --rollback ]] || valid_pin "$2" || fail "expected a pinned GHCR digest"

mkdir -p "$state"
lock=$state/$unit.lock
mkdir "$lock" || fail "deployment already locked"
trap 'rm -f "$lock/pin"; rmdir "$lock"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

write_pin() {
  printf 'IMAGE=%s\n' "$2" > "$lock/pin"
  mv -f "$lock/pin" "$1"
}

restart_and_gate() {
  systemctl restart "$unit" || return 1
  local attempt status
  for ((attempt=0; attempt<10; attempt++)); do
    if systemctl is-active --quiet "$unit" &&
      [[ $(docker container inspect --format '{{.State.Running}} {{.Config.Image}}' "$name") == "true $1" ]]; then
      if [[ $name == quote-collector ]]; then
        return 0
      fi
      status=$(curl --silent --max-time 2 --output /dev/null --write-out '%{http_code}' \
        http://127.0.0.1:8792/healthz) || status=
      [[ $status == 200 ]] && return 0
    fi
    if ((attempt<9)); then sleep 1; fi
  done
  return 1
}

write_pin "$current" "$pin"
if restart_and_gate "$pin"; then
  if [[ -n $old && $old != "$pin" ]]; then
    write_pin "$previous" "$old"
  fi
  echo "deploy-serve: $unit $pin"
  exit 0
fi

if [[ -z $old ]]; then
  rm -f "$current"
  systemctl stop "$unit" || fail "first deployment failed and unit stop failed"
  fail "first deployment failed; unit stopped"
fi
write_pin "$current" "$old"
restart_and_gate "$old" || fail "deployment failed and recovery failed"
fail "deployment failed; restored $old"
