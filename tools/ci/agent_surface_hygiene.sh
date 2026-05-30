#!/usr/bin/env bash
set -euo pipefail

orient_bin="${1:?usage: agent_surface_hygiene.sh <orient-binary>}"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

outputs=()
for profile in codex claude amp generic; do
  instructions="${scratch}/agent-instructions-${profile}.txt"
  guide="${scratch}/agent-guide-${profile}.json"
  "${orient_bin}" agent-instructions --profile "${profile}" >"${instructions}"
  "${orient_bin}" agent-guide --profile "${profile}" >"${guide}"
  outputs+=("${instructions}" "${guide}")
done

"${orient_bin}" tool-manifest >"${scratch}/tool-manifest.json"
"${orient_bin}" mcp-manifest >"${scratch}/mcp-manifest.json"
outputs+=("${scratch}/tool-manifest.json" "${scratch}/mcp-manifest.json")

fixed_patterns=(
  'analytics'
  'session analytics'
  'session data'
  'session-data'
  'session logs'
  'transcript analytics'
  'agent transcripts'
  'agent conversations'
  'task outcomes'
  'tool-call histories'
  'memory/rules surface'
  'agent memory'
  'agent rule'
  'agent-rule'
  'local rule'
  'reusable rules'
  'rule_target'
  'agent_rules'
)

regex_patterns=(
  '/Users/[^[:space:])"]+'
  '/private/tmp[^[:space:])"]*'
  '/var/folders[^[:space:])"]*'
  'Documents/Projects'
  'C:\\Users\\[^[:space:])"]+'
)

for pattern in "${fixed_patterns[@]}"; do
  if rg -n --fixed-strings --ignore-case "$pattern" "${outputs[@]}"; then
    echo "generated agent surface contains private or out-of-scope wording: $pattern" >&2
    exit 1
  fi
done

for pattern in "${regex_patterns[@]}"; do
  if rg -n --ignore-case "$pattern" "${outputs[@]}"; then
    echo "generated agent surface contains private or machine-specific paths: $pattern" >&2
    exit 1
  fi
done

for file in "${scratch}"/agent-instructions-*.txt; do
  grep -q 'does not collect telemetry' "${file}"
  grep -q 'local code-discovery' "${file}"
done

if rg -n 'AGENTS.md|CLAUDE.md|Amp rules' "${outputs[@]}"; then
  echo "generated agent surface should not name adapter-specific rule files" >&2
  exit 1
fi
