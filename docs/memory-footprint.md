# Memory And Footprint

Orient optimizes for local agent latency. Persisted indexes intentionally keep
enough source snapshot and line-offset data to answer search snippets and
bounded range reads without reopening every live file.

## What Is Stored

Each persisted index contains:

- file metadata and path terms
- content-token postings
- trigram postings for substring-style planning
- exact symbol and symbol-kind postings
- source snapshots for bounded snippet and range reads
- line-offset and token-to-line tables
- repo/dependency/import metadata used by filters and repo maps

This makes reads reliable when a daemon is shared across many agents, but it
means the index can be several times larger than source.

## Inspect Footprint

For one repo:

```bash
orient index-status --index /tmp/orient.index
```

For shards:

```bash
orient shard-status --index-dir /tmp/orient-shards --summary
```

Important counters:

- `index_bytes`: total bytes of persisted index files
- `source_bytes`: live source bytes represented by the index
- `content_snapshot_bytes`: bytes held for indexed source snapshots
- `line_offset_bytes`: bytes used for line/range lookup tables
- `posting_entries`: logical posting-list entries
- `compressed_posting_bytes`: compressed posting-map bytes
- `largest_shards`: largest repos by index footprint

## Example Shape

On a large local workspace, expect the broad shape to look like this:

- persisted indexes can be several times larger than the source snapshot
- line-offset tables are meaningful but usually smaller than source snapshots
- compressed postings are visible separately from total index bytes
- warm cached shard search should remain in the low tens of milliseconds for
  common top-10 queries

That result is the core tradeoff: build and disk cost are non-trivial, but a
single warmed daemon amortizes them across many local agent sessions.

## Practical Defaults

Use `--family-limit` when indexing a broad workspace with many repeated
worktrees:

```bash
orient ensure-shards \
  --discover-root ~/code \
  --output-dir /tmp/orient-shards \
  --family-limit 2
```

Use `--family-limit 1` when you only need representative canonical repos and
want a smaller shard directory. Use a higher limit when active worktrees matter.

Keep shard directories outside the repo, usually under `/tmp` or another local
cache location. Do not commit them.

## Memory Notes

The current saved index has an mmap-backed load path, compressed posting maps,
and cached daemon reuse, but search still works with owned Rust structures after
load. The next large-monorepo improvement is a sectioned format where queries
touch only the needed dictionaries/posting blocks instead of decoding most of an
index into memory.

Until then, the recommended production shape is:

- build or refresh shards explicitly
- keep one daemon warm
- reuse that daemon from every local agent
- monitor `shard-status --summary`
- use `refresh_if_stale:true` for searches that must see live edits
