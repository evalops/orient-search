#!/usr/bin/env bash
set -euo pipefail

orient_bin="${1:?usage: orient_bazel_smoke.sh <orient-binary>}"
"${orient_bin}" tool-manifest | grep -q '"name":"search_code"'

printf '%s\n' \
  '{"jsonrpc":"2.0","id":"init","method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{}}' \
  '{"jsonrpc":"2.0","id":"call","method":"tools/call","params":{"name":"list_tools","arguments":{}}}' \
  | "${orient_bin}" serve-mcp \
  | grep -q '"structuredContent"'
