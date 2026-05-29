#!/usr/bin/env bash
set -euo pipefail

docs=(README.md docs)

patterns=(
  '/Users/'
  'Documents/Projects'
  'agent-jsonl-explorer'
  'session analytics'
  'codex jsonl'
  'claude jsonl'
)

for pattern in "${patterns[@]}"; do
  if rg -n --fixed-strings --ignore-case --glob '*.md' "$pattern" "${docs[@]}"; then
    echo "public docs contain private or out-of-scope wording: $pattern" >&2
    exit 1
  fi
done
