#!/usr/bin/env bash
set -euo pipefail

docs=(README.md docs)

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
)

for pattern in "${fixed_patterns[@]}"; do
  if rg -n --fixed-strings --ignore-case --glob '*.md' "$pattern" "${docs[@]}"; then
    echo "public docs contain private or out-of-scope wording: $pattern" >&2
    exit 1
  fi
done

for pattern in "${regex_patterns[@]}"; do
  if rg -n --ignore-case --glob '*.md' "$pattern" "${docs[@]}"; then
    echo "public docs contain private or machine-specific paths: $pattern" >&2
    exit 1
  fi
done

markdown_files=(README.md)
while IFS= read -r file; do
  markdown_files+=("$file")
done < <(find docs -type f -name '*.md' | sort)

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
