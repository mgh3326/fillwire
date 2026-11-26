#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
owner=${2:-}
sha=${3:-}
if [[ $# != 3 || ! $owner =~ ^[A-Za-z0-9-]+$ || ! $sha =~ ^[0-9a-f]{40}$ ]]; then
  exit 64
fi
repository=ghcr.io/${owner,,}/fillwire
tag="$repository:sha-$sha"
case "$mode" in
  --dry-run) printf '%s\n' "$tag"; exit 0 ;;
  --push) ;;
  *) exit 64 ;;
esac

metadata=$(mktemp)
trap 'rm -f "$metadata"' EXIT
docker buildx build --push --platform linux/amd64,linux/arm64 --tag "$tag" \
  --label "org.opencontainers.image.revision=$sha" --metadata-file "$metadata" .
digest=$(jq -er '."containerimage.digest" | select(test("^sha256:[0-9a-f]{64}$"))' "$metadata")
printf 'Pushed image: %s\n' "$repository@$digest"
printf '### GHCR image\n- tag: `%s`\n- deploy pin: `%s@%s`\n' \
  "$tag" "$repository" "$digest" >> "${GITHUB_STEP_SUMMARY:?}"
