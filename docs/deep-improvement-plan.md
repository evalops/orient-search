# Deep Improvement Plan

Orient is already useful as a fast local search layer. The remaining work should
make shared use smoother, reduce refresh/build cost, and keep memory/disk
footprint visible.

Orient should remain local code search only. No session analytics, no telemetry,
and no hosted dependency.

## Current Priorities

### 1. Make Multi-Agent Use Boring

Many local agents often work on the same few codebases. Orient should feel like
one shared local search appliance for that setup.

Done pieces:

- TCP, Unix socket, stdio JSON-lines, and MCP stdio transports
- `serve-tcp` / `serve-unix` startup warming via `--index` and `--index-dir`
- `daemon_status` with warmed targets and copyable default requests
- `doctor` with reachability, freshness, and repair/start commands
- cached index and shard-manifest reloads when persisted files change
- shard write locks and shrink guards for shared shard directories

Next useful work:

- clearer bootstrap command aliases for common local layouts
- better daemon-status summaries when many shards are warmed
- stronger stale-target messaging in agent-generated instructions

### 2. Make Refresh Cheaper Than Rebuild

Wide shard build cost is the main pain left. Warm search is already fast, so
the daemon should amortize indexing and avoid unnecessary rebuilds.

Done pieces:

- incremental single-repo refresh for add/edit/delete/rename
- `ensure-shards` adds missing repos and refreshes existing shards
- `refresh-shards` prunes missing roots and updates nested aliases
- wide perf script reports build seconds and shard footprint

Next useful work:

- parallelize independent shard refresh more aggressively while respecting the
  writer lock
- expose per-shard refresh duration and changed-file counts in summary output
- add a cheap "what would refresh?" dry run for broad shard directories

### 3. Keep Footprint Visible

The current index stores source snapshots and line offsets so agents can read
bounded context without touching live files. That is intentionally fast but not
small.

Done pieces:

- `index_status`, `shard_status`, and `daemon_status` expose index/source,
  content-snapshot, line-offset, posting-entry, and compressed-posting counters
- `shard-status --summary` reports largest shards without huge per-shard output
- wide perf runs print footprint counters before latency gates

Next useful work:

- sectioned mmap-friendly index format with a file metadata section, string
  table, term dictionary, compressed posting blocks, line-offset table, and
  snapshot content blob
- lazy query-time loading for posting blocks
- memory-oriented benchmarks for cold load and warmed daemon RSS

### 4. Improve Query Recovery

The product gets more useful when failed or noisy searches explain what to do
next.

Done pieces:

- query plans with missing terms/trigrams, filter rejection counts, phrase and
  final-match diagnostics
- retry requests for safe repairs
- facet hints for indexed and shard searches

Next useful work:

- richer path/language/test/source/generated facets for noisy shard results
- avoid surfacing hints unless they materially reduce the candidate set
- make broad-result explanations shorter for agents

## Performance Targets

- broad-workspace fallback top-10 p95 <= 300ms for common queries
- repo-local fallback p95 <= 100ms
- indexed repeated-query p95 faster than fallback
- warm shard top-10 searches in the low tens of milliseconds for the local broad
  shard set
- no multi-second hangs; candidate collection has caps and hard timeouts

## Verification

Keep releases backed by:

```bash
cargo fmt --check
cargo test
cargo build --release
bazel test --test_output=errors //...
bazel run //:ci_perf_gates
ORIENT_WIDE_SHARDS=0 bazel run //:ci_wide_perf
```

For shared daemon and footprint changes, also run:

```bash
orient doctor --index-dir /tmp/orient-shards
orient daemon-status
orient shard-status --index-dir /tmp/orient-shards --summary
ORIENT_WIDE_FALLBACK=0 ORIENT_WIDE_SHARDS=1 bazel run //:ci_wide_perf
```
