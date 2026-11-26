#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sha=0123456789abcdef0123456789abcdef01234567
test "$(bash scripts/publish-image.sh --dry-run Mgh3326 "$sha")" = "ghcr.io/mgh3326/fillwire:sha-$sha"
if bash scripts/publish-image.sh --dry-run Mgh3326 main; then
  exit 1
fi
echo "publish-image tagging: PASS"
