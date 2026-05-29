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

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/orient-perf-gates.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT
index_path="${tmpdir}/orient.index"
fallback_bench="${tmpdir}/orient-fallback-bench.json"
indexed_bench="${tmpdir}/orient-indexed-bench.json"
shard_dir="${tmpdir}/orient-shards"

target/release/orient bench-search \
  --repo . \
  --runs 5 \
  --warmup 1 \
  --limit 10 \
  --fail-p95-ms 1000 \
  "indexed search symbol filters" \
  "read range tool manifest" \
  "file:Cargo.toml"

target/release/orient index --repo . --output "${index_path}"
target/release/orient bench-search \
  --repo . \
  --index "${index_path}" \
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
  > "${fallback_bench}"
target/release/orient bench-search \
  --repo . \
  --index "${index_path}" \
  --runs 7 \
  --warmup 2 \
  --limit 10 \
  --baseline "${fallback_bench}" \
  --allow-baseline-mode-mismatch \
  --require-faster-than-baseline \
  --max-p95-regression 0 \
  "indexed search symbol filters" \
  "read range tool manifest" \
  > "${indexed_bench}"

target/release/orient ensure-shards \
  --repo . \
  --output-dir "${shard_dir}"
target/release/orient bench-shards \
  --index-dir "${shard_dir}" \
  --cached \
  --runs 5 \
  --warmup 1 \
  --limit 10 \
  --fail-p95-ms 1000 \
  "indexed search symbol filters" \
  "file:Cargo.toml"
