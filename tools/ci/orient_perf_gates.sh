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
  "indexed search symbol filters" \
  "read range tool manifest" \
  > /tmp/orient-indexed-bench.json
python3 - <<'PY'
import json
import sys

with open("/tmp/orient-fallback-bench.json", "r", encoding="utf-8") as handle:
    fallback = json.load(handle)
with open("/tmp/orient-indexed-bench.json", "r", encoding="utf-8") as handle:
    indexed = json.load(handle)

fallback_by_query = {item["query"]: item for item in fallback["queries"]}
failures = []
for item in indexed["queries"]:
    query = item["query"]
    fallback_item = fallback_by_query[query]
    if item["p95_ms"] >= fallback_item["p95_ms"]:
        failures.append(
            f"{query}: indexed p95 {item['p95_ms']}ms >= fallback p95 {fallback_item['p95_ms']}ms"
        )
if failures:
    print("indexed search did not beat fallback:", file=sys.stderr)
    for failure in failures:
        print(f"  {failure}", file=sys.stderr)
    sys.exit(1)
PY

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
