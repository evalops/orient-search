#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${USER:-}" ]]; then
  user_home="$(eval echo "~${USER}")"
else
  user_home="${HOME}"
fi
export RUSTUP_HOME="${RUSTUP_HOME:-${user_home}/.rustup}"
export CARGO_HOME="${CARGO_HOME:-${user_home}/.cargo}"
export PATH="/etc/profiles/per-user/${USER:-}/bin:${CARGO_HOME}/bin:${user_home}/go/bin:/usr/local/bin:/opt/homebrew/bin:${PATH}"
cd "${BUILD_WORKSPACE_DIRECTORY:-$(pwd)}"
cargo build --release

smoke_repo="$(mktemp -d)"
trap 'rm -rf "${smoke_repo}"' EXIT
mkdir -p "${smoke_repo}/src"
printf '%s\n' \
  "pub fn mcp_dispatch_value() -> &'static str { \"orient smoke fixture\" }" \
  >"${smoke_repo}/src/server.rs"

printf '%s\n' '{"id":"tools","tool":"list_tools","arguments":{}}' \
  | target/release/orient serve-jsonl \
  | grep -q '"search_code"'

jsonl_search_output="$(
  printf '%s\n' \
    "{\"id\":\"search\",\"tool\":\"search_code\",\"arguments\":{\"repo\":\"${smoke_repo}\",\"query\":\"mcp_dispatch_value\",\"limit\":5}}" \
    | target/release/orient serve-jsonl
)"
grep -q '"path":"src/server.rs"' <<<"${jsonl_search_output}"
grep -q '"tool":"read_range"' <<<"${jsonl_search_output}"

mcp_output="$(
  printf '%s\n' \
  '{"jsonrpc":"2.0","id":"init","method":"initialize","params":{}}' \
  '{"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{}}' \
  '{"jsonrpc":"2.0","id":"call","method":"tools/call","params":{"name":"list_tools","arguments":{}}}' \
  "{\"jsonrpc\":\"2.0\",\"id\":\"search\",\"method\":\"tools/call\",\"params\":{\"name\":\"search_code\",\"arguments\":{\"repo\":\"${smoke_repo}\",\"query\":\"mcp_dispatch_value\",\"limit\":5}}}" \
  | target/release/orient serve-mcp
)"
grep -q '"structuredContent"' <<<"${mcp_output}"
grep -q '"path":"src/server.rs"' <<<"${mcp_output}"
grep -q '"tool":"read_range"' <<<"${mcp_output}"
