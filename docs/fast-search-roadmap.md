# Fast Search Roadmap

Orient is a Rust-native local search layer for coding agents. The design is
Zoekt-inspired, but the interface is optimized for tool calls: repo maps,
indexed search, query plans, bounded reads, and structured follow-up requests.

## Product Thesis

Agents already search. The useful work is making local search fast, cheap, and
structured enough that agents stop spending turns on repeated `rg`, `find`,
`ls`, and `cat` exploration.

## Already In Place

- Live `rg`-backed search with Rust-side scoring and snippets.
- Persistent local indexes with token, path, trigram, symbol, symbol-kind, and
  filter postings.
- Incremental single-repo refresh for add, edit, delete, and rename cases.
- Multi-repo shard directories with one index per repo and a validated manifest.
- TCP, Unix-socket, stdio JSON-lines, and MCP-style transports.
- A shared daemon that can register shard directories, lazily warm touched
  shard indexes, and serve many local agents.
- Repo maps, related-file lookup, related-symbol lookup, and bounded range
  reads from live repos, indexes, and shard directories.
- Query plans with missing-term diagnostics, filter rejection counts, and safe
  retry requests.
- Footprint counters for index size, represented source bytes, snapshots, line
  offsets, postings, and largest shards.
- Bazel-backed CI for build, tests, smoke checks, and performance gates.

## Near-Term Direction

- Keep the no-index `rg` path as the baseline fallback.
- Keep improving shard fanout so impossible searches avoid cold index loads.
- Make query-plan output shorter and more actionable for agents.
- Continue moving the persisted format toward sectioned, mmap-friendly blocks.
- Keep docs focused on local setup, shared daemon operation, and footprint.

## Performance Targets

- Broad local fallback search: top-10 p95 at or below `300ms` for common
  literal/token queries.
- Repo-local fallback: p95 at or below `100ms` after warmup.
- Indexed repeated queries should beat fallback queries.
- Warm shard search should stay in the low tens of milliseconds for common
  top-10 queries.
- Candidate collection must stay bounded; no multi-second hangs.

## Verification

```bash
cargo fmt --check
cargo test
cargo build --release
bazel test --test_output=errors //...
bazel run //:ci_perf_gates
ORIENT_WIDE_SHARDS=0 bazel run //:ci_wide_perf
```

For shared daemon or footprint changes, also run:

```bash
orient doctor --index-dir /tmp/orient-shards
orient daemon-status
orient shard-status --index-dir /tmp/orient-shards --summary
```

## Non-Goals

- Hosted indexing.
- Telemetry.
- Replacing specialized shell tools when a direct command is clearly better.
