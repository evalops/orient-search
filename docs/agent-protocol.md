# Agent Protocol

Orient's JSON-lines protocol is meant for local coding agents that need fast search, bounded context reads, and repo orientation without repeatedly crawling the same filesystem.

## Transport

Run either a one-shot stdio server or a shared TCP daemon:

```bash
target/release/orient serve-jsonl
target/release/orient serve-tcp --addr 127.0.0.1:8796 --index-dir /tmp/orient-shards
target/release/orient serve-tcp --addr 127.0.0.1:8796 --ensure-shards-dir /tmp/orient-shards --repo /path/to/repo-a --repo /path/to/repo-b
target/release/orient client-jsonl --addr 127.0.0.1:8796
target/release/orient serve-unix --socket /tmp/orient.sock --index-dir /tmp/orient-shards
target/release/orient client-jsonl --socket /tmp/orient.sock
```

Each request is one JSON object per line:

```json
{"id":"search","tool":"search_shards","arguments":{"index_dir":"/tmp/orient-shards","query":"repo:platform session token auth","limit":5,"require_all":true}}
```

Responses preserve `id` and return either `result` or `error`. Use `tool_manifest` for the complete tool list, argument metadata, daemon-default hints, defaults, enums, and JSON-schema-like input schemas.
Adapters that want MCP-shaped definitions can call `mcp_manifest` or `orient mcp-manifest`; it returns `tools` entries with `name`, `description`, and `inputSchema`.

## Bootstrap

For one repo:

```json
{"id":"ensure","tool":"ensure_index","arguments":{"repo":"/path/to/repo","index":"/tmp/orient.index"}}
{"id":"warm","tool":"warm_index","arguments":{"index":"/tmp/orient.index"}}
```

For many repos:

```json
{"id":"ensure-shards","tool":"ensure_shards","arguments":{"output_dir":"/tmp/orient-shards","discover_roots":["/Users/jonathanhaas/Documents/Projects"],"max_depth":4,"discover_limit":500,"family_limit":2}}
{"id":"status","tool":"daemon_status","arguments":{}}
```

`daemon_status` reports warmed index and shard details so multiple local agents can confirm they share the intended codebase set. When exactly one index or shard directory is warmed, indexed and shard tools marked with `daemon_default.source` may omit `index` or `index_dir`; if zero or multiple targets are warmed, pass the path explicitly. Orient does not expose session analytics.

Use `index_status` or `shard_status` when live files may have changed since indexing. They report added, changed, and deleted files so an agent can call `refresh_index` or `refresh_shards` before trusting indexed results. `indexed_search_code` and `search_shards` also accept `refresh_if_stale:true` for a one-call freshness check and refresh before search. Index, shard, and daemon status outputs include footprint counters such as `source_bytes`, `posting_entries`, and `compressed_posting_bytes`.

## Search First

Use the fastest surface that matches your setup:

- `search_code` for a live repo without a prebuilt index.
- `indexed_search_code` for one persistent repo index.
- `search_shards` for a multi-repo shard directory.
- `search_batch`, `indexed_search_batch`, or `search_shards_batch` when an agent wants to try several query formulations in one round trip. CLI equivalents are `search-batch`, `indexed-search-batch`, and `search-shards-batch`.

CLI-style JSON-lines aliases are accepted for the most guessable names: `search` for `search_code`, `indexed_search` for `indexed_search_code`, `index_plan` for `indexed_query_plan`, and `shard_plan` for `shard_query_plan`.

Query strings support filters such as `repo:platform`, `path:src/auth` or `dir:src/auth`, `file:auth.rs`, `lang:rust`, `ext:rs`, `symbol:SessionManager`, `kind:function`, `dep:react`, `import:crate::auth`, `test:false`, negative filters like `-path:docs`, `-kind:class`, `-dep:legacy`, or `-import:old_api`, and quoted phrases like `"issue token"`. Multi-token queries use AND behavior when appropriate.

Search results include:

- `path`: the repo-relative, index-relative, or shard-prefixed path.
- `snippet`: line-numbered context.
- `line_range`: displayed snippet bounds.
- `match_lines`: exact hit lines when available.
- `read_range`: a ready-to-pass `{path,start,lines}` follow-up range.
- `context`: optional attached file context when `context_lines` is set.
- `explanation` and `query_plan` when `explain` is set.

Search `limit` values must be positive and stay under `limit.maximum`; `context_lines`, read ranges, and non-empty batch arrays are bounded by the manifest too, so broad requests fail fast instead of expanding silently.

## Read Next

For most agents, the handoff is:

1. Call search.
2. Collect one or more `read_range` objects from results.
3. Pass them directly to the matching batch read tool.

`open_range`, `open_index_range`, and `open_shard_range` are aliases for agents that phrase context fetches as opening a file range.

Examples:

```json
{"id":"read","tool":"read_index_ranges","arguments":{"index":"/tmp/orient.index","ranges":[{"path":"src/auth.rs","start":1,"lines":80}]}}
{"id":"read-shards","tool":"read_shard_ranges","arguments":{"index_dir":"/tmp/orient-shards","ranges":[{"path":"platform/src/auth.rs","start":40,"lines":80}]}}
```

CLI equivalents support repeatable `--range path:start:lines`:

```bash
target/release/orient read-index-ranges --index /tmp/orient.index --range src/auth.rs:1:80
target/release/orient read-shard-ranges --index-dir /tmp/orient-shards --range platform/src/auth.rs:40:80
```

Range reads follow manifest bounds: `start >= 1`, `1 <= lines <= lines.maximum`, non-empty batch arrays, and `ranges.maxItems`, so a mistaken request cannot dump unbounded file content.

## Orientation And Repair

Use `repo_map`, `indexed_repo_map`, or `shard_repo_map` before editing unfamiliar code. They return entrypoints, manifests, tests, important files, top symbols, related files/symbols, command hints, dependency hints, and import/module hints.

For empty or surprising indexed results, call `indexed_query_plan`, `shard_query_plan`, their aliases `index_plan` / `shard_plan`, or their batch forms `indexed_query_plan_batch` / `shard_query_plan_batch`. Plans include active filters with candidate match/rejection counts and separate missing postings, filter rejections, phrase/scoring rejections, and final AND/symbol rejections, with repair hints agents can retry.
