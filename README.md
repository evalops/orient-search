# Orient Search

Orient Search is a local code-search daemon for coding agents. It provides repo
maps, indexed search, query plans, and bounded file reads so agents can inspect
code quickly without repeated filesystem scans.

## Shared Daemon

Run one warmed daemon for a repo set, then point local agents at it:

```bash
cargo install --git https://github.com/evalops/orient-search

orient ensure-shards \
  --discover-root /path/to/workspaces \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

For each client:

```bash
orient doctor --index-dir /tmp/orient-shards
orient agent-instructions --index-dir /tmp/orient-shards
orient daemon-status
orient daemon-status --format json
```

`daemon-status` reports a compact warmed-cache summary. Add `--format json` for
copyable first requests and target details.

## Search

```bash
orient search-auto "symbol:SessionManager token"
orient search-auto --no-daemon "symbol:SessionManager token"
orient search --repo . "issue token"
orient search --index /tmp/repo.index "issue token"
orient search --index-dir /tmp/orient-shards "repo:service issue token"
orient read-range --index /tmp/repo.index src/lib.rs:40:80
```

With no explicit `--repo`, `--index`, or `--index-dir`, `search-auto` first
uses the warm daemon at `127.0.0.1:8796` when available, then falls back to a
live search of the current directory. When run from inside a git checkout, the
daemon request is scoped to that checkout so multi-repo shard daemons stay
focused on the agent's current task. Use `--daemon-addr` for another TCP daemon
or `--no-daemon` to force local fallback.

JSON-lines and MCP-style clients can pass `"cwd": "/path/inside/checkout"` to
no-target `search`, `search_batch`, `search_auto`, `search_auto_batch`,
`repo_map`, `search_plan`, or `find_symbol` calls for the same scoped-daemon
behavior. No-target
`read_range`, `read_ranges`, `related_files`, and `related_symbols` also accept
`cwd` so manual context calls resolve inside the agent's active checkout.
When `cwd` scopes a warmed shard daemon to one checkout, `refresh_if_stale:true`
refreshes that checkout's shard instead of rebuilding every warmed repo.
`shard_status` also accepts `cwd` or an absolute `repo_filter` so shared
daemons can answer freshness for one checkout without opening unrelated shards.

Useful filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `generated:`, `code:`,
`is:test`, `is:source`, `is:code`, `is:docs`, `is:generated`, `content:`,
quoted phrases, negative filters like `-path:vendor`, and `mode:any` for broad
orientation.

Generated paths, including hashed JavaScript bundles, are demoted by default.
Use `generated:true` / `is:generated` when you intentionally want generated
output, or `generated:false` / `-is:generated` to exclude it entirely.

## Protocol

JSON-lines requests look like this:

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/tmp/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"index_dir":"/tmp/orient-shards","detail":"compact","read_limit":16}}
{"id":"search","tool":"search_auto","arguments":{"query":"repo:service branch:main symbol:SessionManager token","limit":10,"explain":true}}
{"id":"read","tool":"read_ranges","arguments":{"index_dir":"/tmp/orient-shards","ranges":[{"path":"service/src/auth.rs","start":40,"lines":80}]}}
```

Every search result includes ready-to-send read, related-file, related-symbol,
and query-plan follow-ups with `jsonl`, `client_cli`, and compact CLI hints.
For manual context reads, pass `scope:"symbol"` or `orient read-range --scope
symbol` to anchor the returned window at the nearest function, class, or type
definition instead of opening an exact line window.

## Footprint

Orient stores source snapshots and line offsets in persisted indexes so bounded
reads stay fast even when served by a shared daemon. This is local-only data:
Orient does not collect telemetry.

Use:

```bash
orient shard-status --index-dir /tmp/orient-shards --summary
```

The summary reports persisted index size, represented source size, snapshot
bytes, line-offset bytes, posting counts, compressed posting bytes, and largest
shards. Indexes are usually larger than source because they keep enough local
state for fast snippets and reads.

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
- [Agent protocol](docs/agent-protocol.md)
- [Fast search roadmap](docs/fast-search-roadmap.md)
