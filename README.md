# Orient Search

Orient Search is a local code-search daemon for coding agents. It provides repo
maps, indexed search, query plans, and bounded file reads so agents can inspect
code quickly without repeated filesystem scans.

## Shared Daemon

Run one shared daemon for the repos an agent is likely to touch:

```bash
cargo install --git https://github.com/evalops/orient-search

export ORIENT_WORKSPACES=/path/to/workspaces
export ORIENT_SHARDS=/path/to/local/cache/orient-shards
export ORIENT_INDEX=/path/to/local/cache/orient.index

orient ensure-shards \
  --discover-root "$ORIENT_WORKSPACES" \
  --output-dir "$ORIENT_SHARDS" \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir "$ORIENT_SHARDS"
```

`--index-dir` registers the shard manifest and lazily loads individual repo
indexes on first use. The daemon keeps at most 64 ready indexes by default; use
`--max-cached-indexes N` to tune that for shared multi-agent sessions. Use
`--warm-index-dir "$ORIENT_SHARDS"` only when you intentionally want to load
shard indexes at startup.

Then verify the daemon and generate the short agent rule:

```bash
orient doctor --index-dir "$ORIENT_SHARDS"
orient agent-instructions --profile codex --index-dir "$ORIENT_SHARDS"
orient daemon-status
orient daemon-status --format json
```

`daemon-status` reports registered shard directories and warmed indexes. Add
`--format json` for copyable first requests and target details.

The daemon shares local search state only: indexes, shard manifests, repo maps,
and cached file metadata. It does not ingest agent transcripts, session logs, or
interaction analytics.

## Search

```bash
orient search-auto "symbol:SessionManager token"
orient search-auto --no-daemon "symbol:SessionManager token"
orient search --repo . "issue token"
orient search --index "$ORIENT_INDEX" "issue token"
orient search --index-dir "$ORIENT_SHARDS" "repo:service issue token"
orient read-range --index "$ORIENT_INDEX" src/lib.rs:40:80
orient read-range --repo . src/lib.rs#L40-L45
```

With no explicit `--repo`, `--index`, or `--index-dir`, `search-auto` first
uses the shared daemon at `127.0.0.1:8796` when available, then falls back to a
live search of the current directory. When run from inside a git checkout, the
daemon request is scoped to that checkout so multi-repo shard daemons stay
focused on the agent's current task and only load matching shard indexes. Use
`--daemon-addr` for another TCP daemon or `--no-daemon` to force local fallback.

`orient client-jsonl` adds the shell's current working directory to no-target
search, map, plan, symbol, read, and related-file calls. Other protocol clients
can pass `cwd` explicitly. The daemon uses that checkout as the default scope,
which keeps shared multi-repo daemons focused on the current task. With the same
scope, `refresh_if_stale:true` refreshes only that repo's shard. Empty or
diagnostic `search_auto` responses include a compact `freshness` object when
the scoped index is stale, plus a top-level ready-to-run `refresh_request` that
refreshes and repeats the search. Shard freshness includes branch/origin
metadata drift, so switching branches without touching files is still detected.
When a JSON-lines or MCP client calls `daemon_status` with `cwd`, the returned
`default_requests` also include that `cwd`, so copyable first map, search,
batch, and query-plan calls stay scoped to the active checkout.

Useful filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `generated:`, `code:`,
`is:test`, `is:source`, `is:code`, `is:docs`, `is:generated`, `content:`,
quoted phrases, negative filters like `-path:vendor`, and `mode:any` for broad
orientation.
Bare filename and path-like queries such as `Cargo.toml` or `src/lib.rs` use the
same fast path filters. Use `content:Cargo.toml` when you want references to the
string instead of the file itself. Pasted locations such as `src/lib.rs:42`,
`src/lib.rs:42:9`, `src/lib.rs#L42-L45`, copied `src/lib.rs:42: text` lines,
Markdown-style file links, common hosted code links, and stack-frame forms like
`at fn (src/lib.rs:42:9)` resolve to the file and anchor snippets near that
line. Absolute pasted paths are normalized when they are inside the selected
repo or index root. Hosted links may carry fragment or query-string line
anchors.
`symbol:` accepts exact names plus strong multi-token identifier fragments, so
`symbol:retry_result` can match `search_auto_retry_result`; single generic tokens
stay exact to avoid broad matches such as `symbol:path` hitting every
`*_path` helper.

Generated paths, including hashed JavaScript bundles, are demoted by default.
Use `generated:true` / `is:generated` when you intentionally want generated
output, or `generated:false` / `-is:generated` to exclude it entirely.

## Protocol

JSON-lines requests look like this:

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/path/to/local/cache/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"index_dir":"/path/to/local/cache/orient-shards","detail":"compact","read_limit":16}}
{"id":"search","tool":"search_auto","arguments":{"query":"repo:service branch:main symbol:SessionManager token","limit":10,"explain":true}}
{"id":"read","tool":"read_ranges","arguments":{"index_dir":"/path/to/local/cache/orient-shards","ranges":[{"path":"service/src/auth.rs","start":40,"lines":80},"service/src/lib.rs#L40-L45"]}}
```

Every search result includes ready-to-send read, related-file, related-symbol,
and query-plan follow-ups with `jsonl`, `client_cli`, and compact CLI hints.
`search_auto` and `search_auto_batch` also expose `next_read_batch_request`,
which points to the best immediate batch read after normal results or an
automatic retry. Their `next_action` field wraps the best immediate follow-up
request with a compact `kind`, `source`, and `summary`.
For manual context reads, pass `scope:"symbol"` or `orient read-range --scope
symbol` to anchor the returned window at the nearest function, class, or type
definition instead of opening an exact line window.
The `read_range` / `read_ranges` protocol tools and `read-range` /
`read-ranges` CLIs accept the same copied file locations as search, including
`src/lib.rs:42`, copied `src/lib.rs:42: text` lines, `src/lib.rs#L42-L45`,
Markdown links, and common hosted code links with fragment or query-string line
anchors.

## Footprint

Orient stores source snapshots and line offsets in persisted indexes so bounded
reads stay fast even when served by a shared daemon. Keep indexes in a local
cache and out of source control. Orient does not collect telemetry or agent
session data.

Use:

```bash
orient shard-status --index-dir "$ORIENT_SHARDS" --summary
```

The summary reports index size, represented source size, snapshot bytes,
line-offset bytes, posting counts, compressed posting bytes, and largest shards.
Indexes are usually larger than source because they keep enough local state for
fast snippets and reads.

## Build And Test

```bash
bazel build -c opt //:orient
bazel test //...
bazel run //:ci_full_test
bazel run //:ci_perf_gates
ORIENT_WIDE_SHARDS=0 bazel run //:ci_wide_perf
```

## Docs

- [Shared daemon guide](docs/shared-daemon.md)
- [Memory and footprint](docs/memory-footprint.md)
- [Agent adoption](docs/agent-adoption.md)
- [Agent protocol](docs/agent-protocol.md)
- [Fast search roadmap](docs/fast-search-roadmap.md)
