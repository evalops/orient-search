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

rm -f /tmp/orient.index /tmp/orient-fallback-bench.json /tmp/orient-indexed-bench.json
rm -rf /tmp/orient-shards

target/release/orient bench-search \
  --repo . \
  --runs 5 \
  --warmup 1 \
  --limit 10 \
  --fail-p95-ms 1000 \
  "indexed search symbol filters" \
  "read range tool manifest" \
  "file:Cargo.toml"

target/release/orient index --repo . --output /tmp/orient.index
target/release/orient bench-search \
  --repo . \
  --index /tmp/orient.index \
  --runs 5 \
  --warmup 1 \
  --limit 10 \
  --fail-p95-ms 500 \
  "indexed search symbol filters" \
  "read range tool manifest"

target/release/orient bench-search \
  --repo . \
  --runs 7 \
  --warmup 2 \
  --limit 10 \
  "indexed search symbol filters" \
  "read range tool manifest" \
  > /tmp/orient-fallback-bench.json
target/release/orient bench-search \
  --repo . \
  --index /tmp/orient.index \
  --runs 7 \
  --warmup 2 \
  --limit 10 \
  --baseline /tmp/orient-fallback-bench.json \
  --allow-baseline-mode-mismatch \
  --require-faster-than-baseline \
  --max-p95-regression 0 \
  "indexed search symbol filters" \
  "read range tool manifest" \
  > /tmp/orient-indexed-bench.json

target/release/orient ensure-shards \
  --repo . \
  --output-dir /tmp/orient-shards
target/release/orient bench-shards \
  --index-dir /tmp/orient-shards \
  --cached \
  --runs 5 \
  --warmup 1 \
  --limit 10 \
  --fail-p95-ms 1000 \
  "indexed search symbol filters" \
  "file:Cargo.toml"
