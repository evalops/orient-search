# Memory And Footprint

Orient optimizes for local agent latency. Persisted indexes store source
snapshots and line metadata so snippets and bounded reads do not need to reopen
live files. This makes reads fast and makes index files larger than source.

## What Gets Stored

Each index contains:

- file metadata and path terms
- content-token, trigram, symbol, and symbol-kind postings
- filter metadata for language, extension, tests, generated files, code/docs,
  dependencies, and imports
- source snapshots for snippets and bounded range reads
- line-offset and token-to-line tables

That tradeoff is intentional: agents can inspect search hits quickly, and a
shared daemon amortizes load cost across sessions.

## Inspect It

```bash
orient index-status --index /tmp/orient.index
orient shard-status --index-dir /tmp/orient-shards --summary
orient daemon-status
orient daemon-status --format json
```

Useful counters:

- `index_bytes`: total persisted index size
- `source_bytes`: source represented by the index
- `content_snapshot_bytes`: stored source snapshot bytes
- `line_offset_bytes`: line/range lookup table bytes
- `posting_entries`: logical posting-list entries
- `compressed_posting_bytes`: compressed posting-map bytes
- `largest_shards`: largest shard indexes in a shard directory

## Defaults

Use `--family-limit` when discovering workspaces with repeated clones or
worktrees:

```bash
orient ensure-shards \
  --discover-root ~/code \
  --output-dir /tmp/orient-shards \
  --family-limit 2
```

Use `--family-limit 1` for a smaller representative shard set. Increase it when
active worktrees matter.

Keep generated indexes outside the repo, such as under `/tmp` or another local
cache directory. Do not commit them.

## Current Shape

The saved index has compressed posting maps, an mmap-backed load path, and
cached daemon reuse. After load, search still uses owned Rust structures. The
next major footprint improvement is a sectioned index format where queries load
only the dictionaries and posting blocks they need.
