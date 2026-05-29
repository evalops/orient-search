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

root="${ORIENT_WIDE_ROOT:-${user_home}/code}"
fallback_p95_ms="${ORIENT_WIDE_FALLBACK_P95_MS:-300}"
shard_p95_ms="${ORIENT_WIDE_SHARD_P95_MS:-300}"
family_limit="${ORIENT_WIDE_FAMILY_LIMIT:-1}"
fallback="${ORIENT_WIDE_FALLBACK:-1}"
shards="${ORIENT_WIDE_SHARDS:-1}"
output_dir="${ORIENT_WIDE_OUTPUT_DIR:-/tmp/orient-wide-shards}"
queries=(
  "search query plan"
  "read range tool"
  "file:Cargo.toml"
)

json_string() {
  local value="${1//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

if [[ ! -d "${root}" ]]; then
  if [[ "${ORIENT_WIDE_REQUIRE_ROOT:-0}" == "1" ]]; then
    echo "wide perf root does not exist: ${root}" >&2
    exit 1
  fi
  echo "skipping wide perf gate; root does not exist: ${root}" >&2
  exit 0
fi

cargo build --release

query_args=()
for query in "${queries[@]}"; do
  query_args+=(--query "${query}")
done

if [[ "${fallback}" == "1" ]]; then
  echo "wide fallback gate: root=${root} p95<=${fallback_p95_ms}ms" >&2
  target/release/orient bench-search \
    --repo "${root}" \
    --mode fallback \
    --runs "${ORIENT_WIDE_RUNS:-5}" \
    --warmup "${ORIENT_WIDE_WARMUP:-1}" \
    --limit 10 \
    --fail-p95-ms "${fallback_p95_ms}" \
    "${query_args[@]}"
else
  echo "skipping wide fallback gate; ORIENT_WIDE_FALLBACK=${fallback}" >&2
fi

if [[ "${shards}" == "1" ]]; then
  rm -rf "${output_dir}"
  echo "wide shard build: root=${root} output_dir=${output_dir} family_limit=${family_limit}" >&2
  build_started_s="$(date +%s)"
  target/release/orient ensure-shards \
    --discover-root "${root}" \
    --output-dir "${output_dir}" \
    --family-limit "${family_limit}"
  build_finished_s="$(date +%s)"
  printf '{"mode":"wide_shard_build","build_seconds":%s,"output_dir":%s}\n' \
    "$((build_finished_s - build_started_s))" \
    "$(json_string "${output_dir}")"
  echo "wide shard status: output_dir=${output_dir}" >&2
  target/release/orient shard-status --index-dir "${output_dir}" --summary
  echo "wide cached shard gate: output_dir=${output_dir} p95<=${shard_p95_ms}ms" >&2
  target/release/orient bench-shards \
    --index-dir "${output_dir}" \
    --cached \
    --runs "${ORIENT_WIDE_SHARD_RUNS:-5}" \
    --warmup "${ORIENT_WIDE_SHARD_WARMUP:-1}" \
    --limit 10 \
    --fail-p95-ms "${shard_p95_ms}" \
    "${query_args[@]}"
fi
