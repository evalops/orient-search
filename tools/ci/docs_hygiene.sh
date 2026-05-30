#!/usr/bin/env bash
set -euo pipefail

docs=(README.md docs)
markdown_files=(README.md)
while IFS= read -r file; do
  markdown_files+=("$file")
done < <(find docs -type f -name '*.md' | sort)

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
  'AGENTS.md'
  'CLAUDE.md'
  'Amp rules'
)

regex_patterns=(
  '/Users/[^[:space:])"]+'
  '/private/tmp[^[:space:])"]*'
  '/var/folders[^[:space:])"]*'
  'Documents/Projects'
  'C:\\Users\\[^[:space:])"]+'
  '\bCodex\b'
  '\bClaude( Code)?\b'
  '\bAmp\b'
)

for pattern in "${fixed_patterns[@]}"; do
  if grep -n -F -i "$pattern" "${markdown_files[@]}"; then
    echo "public docs contain private or out-of-scope wording: $pattern" >&2
    exit 1
  fi
done

for pattern in "${regex_patterns[@]}"; do
  if grep -n -E -i "$pattern" "${markdown_files[@]}"; then
    echo "public docs contain private or machine-specific paths: $pattern" >&2
    exit 1
  fi
done

require_doc_string() {
  local file="$1"
  local needle="$2"
  if ! grep -q -F "$needle" "$file"; then
    echo "$file should document: $needle" >&2
    exit 1
  fi
}

require_doc_string docs/agent-protocol.md 'query_plan_summary'
require_doc_string docs/agent-protocol.md 'next_read_batch_request'
require_doc_string docs/agent-protocol.md 'plan batch items include top-level `summary`'
require_doc_string docs/agent-protocol.md 'Symbol batch items include `summary`'

while IFS=: read -r file line target; do
  target="${target%% \"*}"
  target="${target#<}"
  target="${target%>}"

  case "$target" in
    ''|\#*|http://*|https://*|mailto:*)
      continue
      ;;
  esac

  link_path="${target%%#*}"
  [ -n "$link_path" ] || continue

  if [[ "$link_path" = /* ]]; then
    echo "$file:$line: public docs should not link to absolute local paths: $target" >&2
    exit 1
  fi

  candidate="$(dirname "$file")/$link_path"
  if [ ! -e "$candidate" ]; then
    echo "$file:$line: broken local docs link: $target" >&2
    exit 1
  fi
done < <(
  perl -ne 'while (/\[[^\]]+\]\(([^)]+)\)/g) { print "$ARGV:$.:$1\n" } close ARGV if eof' "${markdown_files[@]}"
)
