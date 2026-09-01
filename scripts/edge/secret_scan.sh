#!/usr/bin/env sh
set -eu

# A small tracked-file heuristic for this public bootstrap. It complements, but
# does not replace, repository-hosted secret scanning.
if git grep -nE -- '-----BEGIN ([A-Z ]+ )?PRIVATE KEY-----'; then
  exit 1
fi
if git grep -nE -- 'AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{20,}'; then
  exit 1
fi
if git grep -nE -- 'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'; then
  exit 1
fi
# Real values in the documented env assignments are forbidden; angle-bracket
# placeholders in .env.example are intentionally permitted.
if git grep -nE -- '^(KIS_MOCK_APP_KEY|KIS_MOCK_APP_SECRET|KIS_MOCK_ACCOUNT_NO|REDIS_URL)=[^<[:space:]#]'; then
  exit 1
fi
printf '%s\n' 'secrets check: clean'
