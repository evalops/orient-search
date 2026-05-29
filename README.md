# Orient Search

Orient Search is a local code-search daemon for coding agents. It gives Codex,
Claude Code, Amp, and other local agents repo maps, indexed search, query plans,
and bounded file ranges so they stop burning runs on repeated `rg`, `find`,
`ls`, and `cat`.

## Shared Daemon

Run one warmed daemon per machine or workspace family, then point every local
agent at it:

```bash
cargo install --git https://github.com/evalops/orient-search

orient ensure-shards \
  --discover-root ~/code \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

In each agent session:

```bash
orient doctor --index-dir /tmp/orient-shards
orient agent-instructions --index-dir /tmp/orient-shards
orient daemon-status
```

`daemon-status` reports the warmed shard/index set, freshness, footprint
counters, and copyable `default_requests` so agents can start with the right
repo map, search, and query-plan calls.

## Search

```bash
orient search-auto "symbol:SessionManager token"
orient search --repo . "issue token"
orient search --index /tmp/repo.index "issue token"
orient search --index-dir /tmp/orient-shards "repo:service issue token"
orient read-range --index /tmp/repo.index src/lib.rs:40:80
```

Useful filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `generated:`, `code:`,
`is:test`, `is:source`, `is:code`, `is:docs`, `is:generated`, `content:`,
quoted phrases, negative filters like `-path:vendor`, and `mode:any` for broad
orientation.

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

## Footprint

Orient stores source snapshots and line offsets in persisted indexes so agents
can read bounded context without touching the live filesystem. That makes
snippet/range reads fast and robust, but index files are larger than source.

Use:

```bash
orient shard-status --index-dir /tmp/orient-shards --summary
```

The summary reports `index_bytes`, `source_bytes`, `content_snapshot_bytes`,
`line_offset_bytes`, `posting_entries`, `compressed_posting_bytes`, and the
largest shards. On large workspaces, expect indexes to be larger than source
because Orient stores snapshots and line offsets for fast bounded reads; warm
top-10 shard searches should still stay in the low tens of milliseconds.

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
