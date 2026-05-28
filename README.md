# Orient Search

Orient Search is a local code-search daemon for coding agents. It gives Codex,
Claude Code, Amp, and other local agents repo maps, indexed search, query plans,
and bounded file ranges so they stop burning runs on repeated `rg`, `find`,
`ls`, and `cat`.

```bash
cargo install --git https://github.com/evalops/orient-search

orient ensure-shards \
  --discover-root ~/Documents/Projects \
  --output-dir /tmp/orient-shards \
  --family-limit 2

orient serve-tcp \
  --addr 127.0.0.1:8796 \
  --index-dir /tmp/orient-shards
```

Agents can talk JSON-lines over TCP, Unix sockets, or stdio:

```bash
orient agent-instructions --index-dir /tmp/orient-shards
orient agent-guide --index-dir /tmp/orient-shards
orient daemon-status --addr 127.0.0.1:8796
orient client-jsonl --addr 127.0.0.1:8796
```

For one-shot local use inside a repo, agents can start with:

```bash
orient search-auto "symbol:AuthSession token"
```

For direct CLI search, the same `search` command works across live repos,
single indexes, and shard directories:

```bash
orient search --repo . "issue token"
orient search --index /tmp/repo.index "issue token"
orient search --index-dir /tmp/orient-shards "repo:api issue token"
```

```jsonl
{"id":"tools","tool":"tool_manifest","arguments":{}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/tmp/orient-shards"}}
{"id":"map","tool":"shard_repo_map","arguments":{"symbols":25,"tests":25,"detail":"compact","read_limit":16}}
{"id":"search","tool":"search","arguments":{"index_dir":"/tmp/orient-shards","query":"repo:api issue token","limit":10}}
{"id":"batch","tool":"search_batch","arguments":{"index_dir":"/tmp/orient-shards","queries":["repo:api issue token","repo:api path:auth token"],"limit":10}}
{"id":"auto","tool":"search_auto","arguments":{"query":"repo:api symbol:AuthSession token","limit":10,"explain":true}}
{"id":"autos","tool":"search_auto_batch","arguments":{"queries":["repo:api symbol:AuthSession token","repo:api path:auth token"],"limit":10}}
{"id":"shards","tool":"search_shards","arguments":{"query":"repo:api symbol:AuthSession token","limit":10,"explain":true}}
{"id":"read","tool":"read_ranges","arguments":{"index_dir":"/tmp/orient-shards","ranges":[{"path":"api/src/auth.rs","start":40,"lines":80}]}}
```

The intended agent loop is simple: ask for the tool manifest, get a repo map,
search the shard set, read the returned `read_range` objects, and inspect the
query plan when results are empty or noisy. See [Agent adoption](docs/agent-adoption.md)
for copyable Codex, Claude Code, and Amp instructions.
Once a daemon has exactly one shard directory or index warmed, `search_auto`
lets wrappers search that target with just a query. The CLI form defaults to
the current directory when no `--index-dir`, `--index`, or `--repo` is supplied.
The JSON-lines form uses the same live-repo fallback from the daemon process
current directory after explicit and warmed targets.
Use `search_auto_batch` when an agent wants to try several query formulations in
one daemon round trip.
Use `search-batch --repo`, `search-batch --index`, or `search-batch --index-dir`
for the same target-aware plain batch search shape from the CLI.
Both return a `query_plan_request` for noisy result sets and inline
`query_plan_result` diagnostics when an automatic search is empty. Add
`diagnose:true` / `--diagnose` to include the plan even when results exist.
Generated plan, map, and retry follow-ups include `cli` hints too, so a terminal
agent can replay the same repair/orientation step with `orient search-plan`,
`orient repo-map`, or `orient search`.
They also return a `repo_map_request` for quick orientation on the chosen
search surface and a `read_batch_request` when results can be opened in one
bounded batch read.
`read_range` and `read_ranges` accept the same `repo`, `index`, or `index_dir`
target style as `search`, and the CLI mirrors that with `read-range --repo`,
`read-range --index`, or `read-range --index-dir`. Simple adapters do not need
separate read tools for live repos, persisted indexes, and shard directories.
Generated `read_request` and `read_batch_request` objects include a `cli` hint
with the equivalent bounded `orient read-*` command for terminal-native agents.
CLI range reads also accept compact `path:start:lines` positional ranges, e.g.
`orient read-range --index /tmp/repo.index src/lib.rs:40:80`.
`related_files` and `related_symbols` follow the same target style for nearby
tests, source counterparts, definitions, and types; the CLI mirrors this as
`related --repo`, `related --index`, or `related --index-dir`.
Generated `related_request` and `related_symbols_request` objects also include
`cli` hints so terminal agents can jump from a hit to nearby files or symbols
without translating the JSON request shape.
`repo_map` follows it too, returning live, indexed, or shard orientation from
one JSON-lines tool; the CLI mirrors this as `repo-map --repo`,
`repo-map --index`, or `repo-map --index-dir`, with `--format json` accepted
when wrappers want to make the JSON output contract explicit.
`find_symbol` and `find_symbol_batch` also accept `repo`, `index`, or
`index_dir` for direct definition jumps; the CLI mirrors this as `symbol --repo`,
`symbol --index`, or `symbol --index-dir`.
Repo maps default to `detail:"compact"` for small first-orientation payloads;
use `detail:"full"` only when an agent needs the full available import/module
hint set. Their bundled `read_batch_request` defaults to 16 ranges and accepts
`read_limit` up to 64 for deliberate wider reads.

For empty or noisy searches, use `orient search-plan --repo . "query"`,
`orient search-plan --index /tmp/repo.index "query"`, or
`orient search-plan --index-dir /tmp/orient-shards "query"` to get missing-term
and retry diagnostics on the same target style as search.

Filters: `repo:`, `path:`/`dir:`, `file:`, `lang:`, `ext:`, `symbol:`,
`kind:`/`type:`, `dep:`, `import:`, `test:`, `is:test`, `is:source`,
`content:`, `text:`, `-path:docs`, quoted phrases, and `mode:any`.
`file:` and `path:` accept `*` and `?` wildcards; `path:` accepts `/` or `\`
separators. `test:true` recognizes common test/spec directories and filenames
such as `tests/`, `__tests__/`, `spec/`, `_test.go`, `_test.rs`,
`.test.tsx`, and `.spec.ts`.

The adoption eval: run the same 20 repo-editing tasks with and without Orient.
Measure time to first relevant file, local-search command count, wrong file
opens, tool calls before edit, edit success rate, and wall-clock time. See
[Adoption eval](docs/adoption-eval.md).

More: [Agent adoption](docs/agent-adoption.md), [Agent protocol](docs/agent-protocol.md),
[Adoption eval](docs/adoption-eval.md), [Fast search roadmap](docs/fast-search-roadmap.md).
