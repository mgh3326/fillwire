#!/bin/sh
# Pull a tagged broker-edge image, pin its resolved digest for a systemd unit,
# and only retain the pin when the unit's metrics endpoint is healthy.
set -eu

usage() {
  echo "usage: $0 [--rollback] <unit-name> [tag]" >&2
  exit 64
}

fail() {
  echo "deploy-pull: $*" >&2
  exit 1
}

rollback=false
if [ "${1:-}" = "--rollback" ]; then
  rollback=true
  shift
fi

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || usage
unit=$1
tag=${2:-main}

case "$unit" in
  ''|*/*|*'..'*|*[!A-Za-z0-9_.@-]*) usage ;;
esac
case "$tag" in
  ''|*[!A-Za-z0-9_.-]*) usage ;;
esac

state_dir=${BROKER_EDGE_STATE_DIR:-/var/lib/broker-edge}
repository=${BROKER_EDGE_IMAGE_REPOSITORY:-ghcr.io/mgh3326/broker-edge}
health_port=${BROKER_EDGE_HEALTH_PORT:-8080}
health_url=${BROKER_EDGE_HEALTH_URL:-http://127.0.0.1:${health_port}/metrics}
health_attempts=${BROKER_EDGE_HEALTH_ATTEMPTS:-10}
health_interval=${BROKER_EDGE_HEALTH_INTERVAL:-1}

case "$state_dir" in '') fail "BROKER_EDGE_STATE_DIR must not be empty" ;; esac
case "$repository" in ''|*'@'*|*' '*|*'\t'*) fail "invalid BROKER_EDGE_IMAGE_REPOSITORY" ;; esac
case "$health_attempts" in *[!0-9]*|'') fail "BROKER_EDGE_HEALTH_ATTEMPTS must be a positive integer" ;; esac
[ "$health_attempts" -gt 0 ] || fail "BROKER_EDGE_HEALTH_ATTEMPTS must be a positive integer"

image_file=$state_dir/$unit.image
previous_file=$state_dir/$unit.image.previous

read_image() {
  file=$1
  [ -f "$file" ] || return 1
  IFS= read -r line < "$file" || return 1
  prefix=IMAGE=$repository@sha256:
  case "$line" in "$prefix"*) ;; *) return 1 ;; esac
  digest=${line#"$prefix"}
  [ "${#digest}" -eq 64 ] || return 1
  case "$digest" in *[!0123456789abcdef]*) return 1 ;; esac
  # An EnvironmentFile must contain exactly the single expected IMAGE entry.
  [ "$(wc -l < "$file" | tr -d ' ')" = "1" ] || return 1
  printf '%s\n' "$line"
}

write_image() {
  value=$1
  umask 077
  tmp=$(mktemp "$state_dir/.${unit}.image.XXXXXX")
  printf '%s\n' "$value" > "$tmp"
  mv "$tmp" "$image_file"
}

copy_or_remove() {
  source=$1
  target=$2
  if [ -f "$source" ]; then
    cp "$source" "$target"
  else
    rm -f "$target"
  fi
}

health_gate() {
  attempt=1
  while [ "$attempt" -le "$health_attempts" ]; do
    status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' "$health_url") || status=
    if [ "$status" = 200 ]; then
      return 0
    fi
    attempt=$((attempt + 1))
    [ "$attempt" -le "$health_attempts" ] && sleep "$health_interval"
  done
  return 1
}

restart_and_gate() {
  systemctl restart "$unit"
  health_gate
}

mkdir -p "$state_dir"

if "$rollback"; then
  previous=$(read_image "$previous_file") || fail "no valid previous image for $unit"
  current=$(read_image "$image_file") || fail "no valid current image for $unit"

  backup_dir=$(mktemp -d "$state_dir/.${unit}.rollback.XXXXXX")
  trap 'rm -rf "$backup_dir"' EXIT HUP INT TERM
  cp "$image_file" "$backup_dir/current"
  cp "$previous_file" "$backup_dir/previous"
  write_image "$previous"
  cp "$backup_dir/current" "$previous_file"

  if restart_and_gate; then
    echo "deploy-pull: rolled back $unit to ${previous#IMAGE=}"
    exit 0
  fi

  copy_or_remove "$backup_dir/current" "$image_file"
  copy_or_remove "$backup_dir/previous" "$previous_file"
  if restart_and_gate; then
    fail "rollback health gate failed; restored $unit to ${current#IMAGE=}"
  fi
  fail "rollback health gate failed and restoration health gate also failed for $unit"
fi

docker pull "$repository:$tag"
digest=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$repository:$tag" \
  | awk -v prefix="$repository@sha256:" '$0 ~ "^" prefix "[0-9a-f]{64}$" { print; exit }')
[ -n "$digest" ] || fail "could not resolve a RepoDigest for $repository:$tag"
new_image=IMAGE=$digest

backup_dir=$(mktemp -d "$state_dir/.${unit}.deploy.XXXXXX")
trap 'rm -rf "$backup_dir"' EXIT HUP INT TERM
copy_or_remove "$image_file" "$backup_dir/current"
copy_or_remove "$previous_file" "$backup_dir/previous"

if old_image=$(read_image "$image_file"); then
  cp "$image_file" "$previous_file"
else
  rm -f "$previous_file"
fi
write_image "$new_image"

if restart_and_gate; then
  echo "deploy-pull: deployed $unit to $digest"
  exit 0
fi

if [ ! -f "$backup_dir/current" ]; then
  fail "health gate failed for $unit and no previous image is available"
fi
copy_or_remove "$backup_dir/current" "$image_file"
copy_or_remove "$backup_dir/previous" "$previous_file"
if restart_and_gate; then
  fail "health gate failed; restored $unit to ${old_image#IMAGE=}"
fi
fail "health gate failed and rollback health gate also failed for $unit"
