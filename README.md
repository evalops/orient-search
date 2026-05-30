# Orient Search

Orient Search is a local code-search daemon for coding agents. It provides repo
maps, indexed search, query plans, and bounded file reads so agents can inspect
code quickly without repeated filesystem scans. It stores local code-search
artifacts only and has no telemetry.

## Shared Daemon

Run one shared daemon for the repos local agents are likely to touch:

```bash
cargo install --git https://github.com/evalops/orient-search
orient --version

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
indexes on first use. The daemon keeps at most 64 ready indexes and uses at most
8 shard workers per query by default; set `--max-cached-indexes N` and
`ORIENT_MAX_SHARD_WORKERS=N` to tune shared multi-agent runs. Use
`--warm-index-dir "$ORIENT_SHARDS"` only when you intentionally want to load
shard indexes at startup.

Then verify the daemon and generate the short instruction snippet:

```bash
orient doctor --index-dir "$ORIENT_SHARDS"
orient agent-instructions --profile generic --index-dir "$ORIENT_SHARDS"
orient daemon-status
orient daemon-status --format json
```

When using a Unix socket daemon, pass `--socket "$ORIENT_SOCKET"` or export
`ORIENT_SOCKET` before generating instructions.

`daemon-status` reports the daemon version, process id, uptime, shard worker cap,
registered shard directories, and warmed indexes. If the version is missing or
differs from `orient --version`, restart the shared daemon. The JSON-lines
`daemon_status` tool is compact by default; pass `details:true` only when you
need cached paths and per-target details.

The daemon shares local search artifacts only: indexes, shard manifests, repo
maps, and cached file metadata. It is not a remote service or a general runtime
state layer.

## Search

```bash
orient search-auto --retry-if-empty --summary "symbol:SessionManager token"
orient search-auto --no-daemon "symbol:SessionManager token"
orient search --repo . "issue token"
orient search --index "$ORIENT_INDEX" "issue token"
orient search --index-dir "$ORIENT_SHARDS" "repo:service issue token"
orient read-range --index "$ORIENT_INDEX" src/lib.rs:40:80
orient read-range --repo . src/lib.rs#L40C9-L45C1
```

With no explicit `--repo`, `--index`, or `--index-dir`, `search-auto` first
uses the shared daemon at `127.0.0.1:8796` when available, then falls back to a
live search of the current directory. When run from inside a git checkout, the
daemon request is scoped to that checkout so multi-repo shard daemons stay
focused on the agent's current task and only load matching shard indexes. Use
`--daemon-addr` or `ORIENT_ADDR` for another TCP daemon, `ORIENT_SOCKET` for a
Unix socket daemon, or `--no-daemon` to force local fallback.

`orient client-jsonl` adds the shell's current working directory to no-target
search, map, plan, symbol, read, and related-file calls. Generated client
commands pass `--require-version` so stale shared daemons fail loudly instead of
serving an older protocol shape. Other protocol clients can pass `cwd`
explicitly. The daemon uses that checkout as the default scope, which keeps
shared multi-repo daemons focused on the current task. With the same scope,
`refresh_if_stale:true` refreshes only that repo's shard. Empty or diagnostic
`search_auto` responses include a compact `freshness` object when the scoped
index is stale, plus a top-level ready-to-run `refresh_request` that refreshes
and repeats the search. Shard freshness includes branch/origin metadata drift,
so switching branches without touching files is still detected.
`client-jsonl` and `daemon-status` also honor `ORIENT_SOCKET` and `ORIENT_ADDR`
when no transport flag is passed, with explicit flags taking precedence.
When a JSON-lines or MCP client calls `daemon_status` with `cwd`, the returned
`default_requests` also include that `cwd`, so copyable first map, search,
batch, and query-plan calls stay scoped to the active checkout. Those scoped
defaults also set `refresh_if_stale:true`, so they refresh only that checkout's
shard before use.

Useful filters include `repo:`, `path:`/`dir:`/`in:`/`under:`, `file:`, `lang:`, `ext:`,
`symbol:`, `kind:`/`type:`, `line:`, `test:`, `generated:`, `code:`,
`content:`, quoted phrases, negative filters like `-path:vendor`, and `mode:any`
for broad orientation. Bare filenames, pasted file locations, in-repo absolute
paths, Python tracebacks, JavaScript stack frames, Markdown links, and hosted
code links resolve to anchored file searches. Go panic stack locations are
accepted too. Pytest node IDs and simple pytest commands such as
`pytest tests/test_auth.py::test_login -q` resolve to the test file. Language
filters include common shorthands such as `lang:rs`, `lang:ts`, `lang:cpp`,
`lang:csharp`, `lang:shell`, `lang:makefile`, and `lang:justfile`.
`kind:target` and `recipe:name` can jump to Makefile targets,
Justfile targets, GitHub Actions jobs, and Bazel BUILD rule names. Pasted Bazel
labels like `//tools/search:orient_cli` and `:agent_smoke_test` infer target
symbol searches too, including inside commands like
`bazel test //tools/search:orient_cli`. `kind:script` and `script:name` can jump
to package.json and pyproject scripts; `package:name` can jump to
package.json packages, Cargo packages, pyproject packages, Go module paths,
Maven coordinates, and Gradle project names; `bin:name`, `example:name`, and
`bench:name` can jump to Cargo manifest entries; `service:name` can jump to
Docker Compose services; `stage:name` can jump to Dockerfile build stages. See
[Agent protocol](docs/agent-protocol.md) for the full query language.

## Protocol

JSON-lines requests look like this:

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/path/to/local/cache/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"index_dir":"/path/to/local/cache/orient-shards","detail":"compact","read_limit":16}}
{"id":"search","tool":"search_auto","arguments":{"query":"repo:service branch:main symbol:SessionManager token","limit":10,"explain":true,"summary":true}}
{"id":"read","tool":"open_ranges","arguments":{"index_dir":"/path/to/local/cache/orient-shards","ranges":[{"path":"service/src/auth.rs","start":40,"lines":80},"service/src/lib.rs#L40-L45"]}}
```

Search results include ready-to-send read, related-file, related-symbol,
repo-map, and query-plan follow-ups with `jsonl`, `client_cli`, and compact CLI
hints. Agents should run those returned requests directly instead of translating
them back into shell search/read commands.

Symbol lookups include per-hit `read_request`; add `include_read_batch:true` or
use `find_symbol_batch` when the next step is opening all matching definitions.

`search_auto`, `search_auto_batch`, and plan batch items expose
`query_plan_summary` or `summary` alongside optional full plans, plus `next_action` when
Orient can choose the best immediate follow-up. Search summaries also surface
grouped duplicate counts when repeated worktree or copied files collapse into a
canonical result. Use those compact fields first; open the full plan only when a
wrapper needs detailed diagnostics. For direct diagnostics, pass JSON-lines
`summary:true` or add `--summary` to `search-auto`, `search-auto-batch`,
`search-plan`, `search-plan-batch`, `index-plan`, or `index-plan-batch`.

Batch read follow-ups include `read_budget` so wrappers can split large reads
before hitting range or line caps. Manual reads accept copied file locations such
as `src/lib.rs:42-45` and can use `scope:symbol` to anchor at the nearest
definition; the read summary reports when a range hit the hard line cap.

## Footprint

Orient stores source snapshots and line offsets in persisted indexes so bounded
reads stay fast even when served by a shared daemon. Keep indexes in a local
cache and out of source control. Indexes contain source text and search metadata,
with no telemetry.

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

The wide perf gate chooses the local projects workspace when it exists, then
falls back to the common code workspace; set `ORIENT_WIDE_ROOT` to force a
different local root.

`bench-search` and `bench-shards` emit per-query samples plus a compact
`summary` with query count, total samples, max p95/p99, max sample, and the
slowest query. Use `--fail-p95-ms` for hard gates and `--baseline` /
`--write-baseline` for regression checks.

## Docs

- [Shared daemon guide](docs/shared-daemon.md)
- [Storage and footprint](docs/storage-footprint.md)
- [Agent adoption](docs/agent-adoption.md)
- [Agent protocol](docs/agent-protocol.md)
- [Fast search roadmap](docs/fast-search-roadmap.md)
