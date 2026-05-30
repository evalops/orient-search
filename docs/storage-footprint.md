# Storage and Footprint

Orient optimizes for local agent latency. Persisted indexes store source
snapshots and line metadata so snippets and bounded reads do not need to reopen
live files. This makes reads fast and makes index files larger than source.
Indexes stay local; Orient does not collect telemetry, prompts, transcripts,
memories, or tool history.

## What Gets Stored

Each index contains:

- file metadata and path terms
- content-token, trigram, symbol, and symbol-kind postings
- filter metadata for language, extension, tests, generated files, code/docs,
  dependencies, and imports
- source snapshots for snippets and bounded range reads
- line-offset and token-to-line tables

That tradeoff is intentional: agents can inspect search hits quickly while a
shared daemon amortizes index-load cost across local clients.

Indexes contain source text. Treat them like local build artifacts for the
repositories they represent: keep them in local cache storage, out of source
control, and away from shared locations unless the underlying source is allowed
there too.

Daemon RAM is just a hot cache for code-search artifacts: loaded indexes, shard
route data, repo metadata, and freshness checks. It is not a general-purpose
state store or session memory.

## Inspect It

```bash
export ORIENT_INDEX=/path/to/local/cache/orient.index
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient index-status --index "$ORIENT_INDEX"
orient shard-status --index-dir "$ORIENT_SHARDS" --summary
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
- `manifest_route_bytes`: compact shard-route sidecar bytes
- `manifest_route_exact_terms` / `manifest_route_trigram_terms`: routeable exact
  and trigram keys in the shard sidecar
- `manifest_route_substring_filter_shards`: shards carrying the long-substring
  Bloom filter used to reject broad trigram false positives before opening
  shard indexes. The route sidecar also carries compact filter sketches so
  language, extension, test, generated, code, and symbol-kind scopes can prune
  routed shard candidates before cold index loads.
- `largest_shards`: largest shard indexes in a shard directory

`manifest.json` is intentionally slim and keeps repo identity, aliases, git
metadata, and index filenames. Dense sketches and route filters live in binary
sidecars so agents do not pay JSON parse costs for hot-path searches.

## Operating Defaults

Use `--family-limit` when discovering workspaces with repeated clones or
worktrees:

```bash
export ORIENT_WORKSPACES=/path/to/workspaces
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient ensure-shards \
  --discover-root "$ORIENT_WORKSPACES" \
  --output-dir "$ORIENT_SHARDS" \
  --family-limit 2
```

Use `--family-limit 1` for a smaller representative shard set. Increase it when
multiple active worktrees matter.

Keep generated indexes outside the repo in local cache storage. Public examples
should use environment variables or placeholders rather than machine-specific
paths.

Generated source and bundle files are still indexed so agents can inspect them
when needed, but they are demoted in normal ranking. Use `generated:true` or
`is:generated` when generated output is the target; use `generated:false` or
`-is:generated` when it should be excluded entirely.

## Current Shape

The saved index has compressed posting maps, an mmap-backed load path, and
cached daemon reuse. After load, search still uses owned Rust structures. The
next major footprint improvement is a sectioned index format where queries load
only the dictionaries and posting blocks they need.
