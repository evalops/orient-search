#!/usr/bin/env bash
set -euo pipefail

orient_bin="${1:?usage: orient_bazel_smoke.sh <orient-binary>}"
smoke_repo="$(mktemp -d)"
trap 'rm -rf "${smoke_repo}"' EXIT
mkdir -p "${smoke_repo}/src"
printf '%s\n' \
  "pub fn mcp_dispatch_value() -> &'static str { \"orient bazel smoke fixture\" }" \
  >"${smoke_repo}/src/server.rs"

"${orient_bin}" tool-manifest | grep -q '"name":"search_code"'

jsonl_search_output="$(
  printf '%s\n' \
    "{\"id\":\"search\",\"tool\":\"search_code\",\"arguments\":{\"repo\":\"${smoke_repo}\",\"query\":\"mcp_dispatch_value\",\"limit\":5}}" \
    | "${orient_bin}" serve-jsonl
)"
grep -q '"path":"src/server.rs"' <<<"${jsonl_search_output}"
grep -q '"tool":"read_range"' <<<"${jsonl_search_output}"

mcp_output="$(
  printf '%s\n' \
  '{"jsonrpc":"2.0","id":"init","method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{}}' \
  '{"jsonrpc":"2.0","id":"call","method":"tools/call","params":{"name":"list_tools","arguments":{}}}' \
  "{\"jsonrpc\":\"2.0\",\"id\":\"search\",\"method\":\"tools/call\",\"params\":{\"name\":\"search_code\",\"arguments\":{\"repo\":\"${smoke_repo}\",\"query\":\"mcp_dispatch_value\",\"limit\":5}}}" \
  | "${orient_bin}" serve-mcp
)"
grep -q '"structuredContent"' <<<"${mcp_output}"
grep -q '"path":"src/server.rs"' <<<"${mcp_output}"
grep -q '"tool":"read_range"' <<<"${mcp_output}"
