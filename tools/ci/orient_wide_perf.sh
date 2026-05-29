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

root="${ORIENT_WIDE_ROOT:-${user_home}/Documents/Projects}"
fallback_p95_ms="${ORIENT_WIDE_FALLBACK_P95_MS:-300}"
shard_p95_ms="${ORIENT_WIDE_SHARD_P95_MS:-300}"
family_limit="${ORIENT_WIDE_FAMILY_LIMIT:-1}"
shards="${ORIENT_WIDE_SHARDS:-1}"
output_dir="${ORIENT_WIDE_OUTPUT_DIR:-/tmp/orient-wide-shards}"

if [[ ! -d "${root}" ]]; then
  if [[ "${ORIENT_WIDE_REQUIRE_ROOT:-0}" == "1" ]]; then
    echo "wide perf root does not exist: ${root}" >&2
    exit 1
  fi
  echo "skipping wide perf gate; root does not exist: ${root}" >&2
  exit 0
fi

cargo build --release

target/release/orient bench-search \
  --repo "${root}" \
  --runs "${ORIENT_WIDE_RUNS:-5}" \
  --warmup "${ORIENT_WIDE_WARMUP:-1}" \
  --limit 10 \
  --fail-p95-ms "${fallback_p95_ms}" \
  "search query plan" \
  "read range tool" \
  "file:Cargo.toml"

if [[ "${shards}" == "1" ]]; then
  rm -rf "${output_dir}"
  target/release/orient ensure-shards \
    --discover-root "${root}" \
    --output-dir "${output_dir}" \
    --family-limit "${family_limit}"
  target/release/orient bench-shards \
    --index-dir "${output_dir}" \
    --cached \
    --runs "${ORIENT_WIDE_SHARD_RUNS:-5}" \
    --warmup "${ORIENT_WIDE_SHARD_WARMUP:-1}" \
    --limit 10 \
    --fail-p95-ms "${shard_p95_ms}" \
    "search query plan" \
    "read range tool" \
    "file:Cargo.toml"
fi
