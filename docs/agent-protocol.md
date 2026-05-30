# Agent Protocol

Orient's JSON-lines protocol is meant for local coding agents that need fast search, bounded context reads, and repo orientation without repeatedly crawling the same filesystem.

## Transport

Run either a one-shot stdio server or a shared TCP daemon:

```bash
export ORIENT_SHARDS=/path/to/local/cache/orient-shards
export ORIENT_INDEX=/path/to/local/cache/orient.index
export ORIENT_SOCKET=/path/to/local/cache/orient.sock
export ORIENT_REPO_A=/path/to/repo-a
export ORIENT_REPO_B=/path/to/repo-b

orient serve-jsonl
orient serve-mcp
orient serve-tcp --addr 127.0.0.1:8796 --index-dir "$ORIENT_SHARDS"
orient serve-tcp --addr 127.0.0.1:8796 --ensure-shards-dir "$ORIENT_SHARDS" --repo "$ORIENT_REPO_A" --repo "$ORIENT_REPO_B"
orient daemon-status
orient daemon-status --format json
orient client-jsonl
orient serve-unix --socket "$ORIENT_SOCKET" --index-dir "$ORIENT_SHARDS"
orient daemon-status --socket "$ORIENT_SOCKET"
orient daemon-status --socket "$ORIENT_SOCKET" --format json
orient client-jsonl --socket "$ORIENT_SOCKET"
```

Each request is one JSON object per line:

```json
{"id":"search","tool":"search_shards","arguments":{"index_dir":"/path/to/local/cache/orient-shards","query":"repo:service session token auth","limit":5,"require_all":true}}
```

Responses preserve `id` and return either `result` or `error`. Use `tool_manifest` for the complete tool list, argument metadata, daemon-default hints, defaults, enums, and JSON-schema-like input schemas.
Adapters that want MCP-shaped definitions can call `mcp_manifest` or `orient mcp-manifest`; it returns `tools` entries with `name`, `description`, `inputSchema`, and `annotations`. Search, read, map, status, and plan tools are marked read-only. Index/shard build, refresh, register, and warm-cache tools are marked non-destructive but not read-only. `orient serve-mcp` exposes the same runtime over stdio JSON-RPC for MCP clients, supporting `initialize`, `tools/list`, and `tools/call`; native JSON-lines remains available through `serve-jsonl`, TCP, or Unix sockets.
Agents and wrappers that want a compact first-use recipe can call `agent_guide` or run `orient agent-guide`; it returns install, shard bootstrap, daemon, client, status, one-shot search, instruction commands, request templates, and follow-up guidance. For copyable local instructions, call `agent_instructions` or run `orient agent-instructions`; it emits a compact local-agent instruction snippet. Use `profile:"generic"` for neutral output, or pass an explicit adapter profile when you want a placement hint for that agent. The profile does not change the search tools.

## Bootstrap

For one repo:

```json
{"id":"ensure","tool":"ensure_index","arguments":{"repo":"/path/to/repo","index":"/path/to/local/cache/orient.index"}}
{"id":"warm","tool":"warm_index","arguments":{"index":"/path/to/local/cache/orient.index"}}
```

For many repos:

```json
{"id":"ensure-shards","tool":"ensure_shards","arguments":{"output_dir":"/path/to/local/cache/orient-shards","discover_roots":["/path/to/workspace"],"max_depth":4,"discover_limit":500,"family_limit":2}}
{"id":"status","tool":"daemon_status","arguments":{}}
{"id":"instructions","tool":"agent_instructions","arguments":{"index_dir":"/path/to/local/cache/orient-shards","profile":"generic"}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/path/to/local/cache/orient-shards","profile":"generic"}}
```

For an existing shard directory, call `register_shards` to cache only the
manifest. Call `warm_shards` only when every shard index should be loaded
immediately.

`daemon_status`, or the direct CLI wrapper `orient daemon-status`, reports the
daemon version, process id, start time, uptime, warmed indexes, registered shard
directories, and `max_cached_indexes` cap. If `daemon_version` is missing or
differs from `orient --version`, restart the shared daemon before relying on
warm cache behavior. The JSON-lines tool is compact by default and omits cached
paths and per-target details; pass `details:true` only when an adapter needs
them. The default CLI output is compact; use `orient daemon-status --format json`
for registered-target details, `search_auto_default`, and copyable
`default_requests`.
When protocol clients pass `cwd` to `daemon_status`, those `default_requests`
include the same `cwd` and use target-aware no-target tools so a shared daemon
scopes map, search, batch, and query-plan requests to the active checkout. They
also set `refresh_if_stale:true`, which refreshes only the scoped shard before
use.
If `repair_requests` is present, run the provided request first; for example,
it can register a shard manifest when only one shard index was warmed.

When exactly one index is warmed or one shard directory is registered, indexed and shard tools
marked with `daemon_default.source` may omit `index` or `index_dir`; otherwise
pass the target explicitly. `search_auto` and `search_auto_batch` use an
explicit `index_dir`, `index`, or `repo` first, then one registered shard directory or warmed index,
then live fallback search from the daemon runtime.

Generated follow-up objects such as `read_request`, `read_batch_request`, `related_request`, `related_symbols_request`, `repo_map_request`, `query_plan_request`, and query-plan `retry_requests` are complete tool requests. They include an `id`, `tool`, `arguments`, raw `jsonl`, a shell-native `client_cli` pipe for `orient client-jsonl`, and, when there is a compact human CLI equivalent, a `cli` hint. Generated batch read requests also include `summary` and `read_budget` with `range_count`, `total_lines`, `max_ranges`, `max_total_lines`, and `max_lines_per_range`, so adapters can inspect, split, or widen reads intentionally.

Use `index_status` or `shard_status` when live files may have changed since
indexing. They report added, changed, deleted files, and shard git metadata
drift such as branch switches, so an agent can refresh before trusting indexed
results. `indexed_search_code` and `search_shards` also accept
`refresh_if_stale:true` for a one-call freshness check and refresh before
search.
For registered shard daemons, `search_auto_batch` coalesces
`refresh_if_stale:true` across the batch: it resolves each query's `cwd`,
`repo:`, `branch:`, or `origin:` scope, refreshes the selected shard roots once,
and then runs all batch items against that shard directory.

For shared shard daemons, pass `cwd` or `repo_filter` when the agent only needs
freshness for one checkout. Status outputs include footprint counters such as
`index_bytes`, `source_bytes`, `content_snapshot_bytes`, `line_offset_bytes`,
`posting_entries`, and `compressed_posting_bytes`; shard status also reports
route-sidecar counters. Use `shard_status --summary` for large shared shard
sets. See [Storage and footprint](storage-footprint.md) for the resource
tradeoffs behind those counters.

Use `ensure_shards` for shard directories shared by several local agents. The lower-level `index_shards` rebuild path refuses to overwrite an existing shard directory when the requested repo set would remove existing shards; pass `force:true` or `orient index-shards --force` only when intentionally replacing that directory.

## Search First

Use the fastest surface that matches your setup:

- `search_auto` when a daemon has exactly one registered shard directory or warmed index, when the request supplies `index_dir`, `index`, or a live `repo`, or when the daemon was started from the desired repo directory. It returns `{query,summary,surface,target,query_plan_request,query_plan_summary,repo_map_request,read_batch_request,next_read_batch_request,next_action,results}` and keeps result follow-up requests aligned with the chosen surface. `summary` carries `status`, `result_count`, top paths, and score bounds; `query_plan_summary` is present when automatic diagnosis or retry planning ran, so wrappers can inspect status and retry intent without parsing the full plan. Shard `query_plan_summary` entries include each shard item's compact `summary` and promoted `next_action` when a retry is available.
- `search_code` for a live repo without a prebuilt index.
- `indexed_search_code` for one persistent repo index.
- `search_shards` for a multi-repo shard directory.
- `search_auto_batch`, `search_batch`, `indexed_search_batch`, or `search_shards_batch` when an agent wants to try several query formulations in one round trip. Each explicit search batch item returns `summary` with `status`, `result_count`, top paths, and score bounds, target-aware `query_plan_request`, `repo_map_request`, read follow-ups, and `next_action`; hits point `next_action` at the batch read request, while empty items point it at the plan request. `search_auto_batch` items also expose compact `query_plan_summary` when diagnostics are attached. On shard paths, `search_auto_batch` and explicit `search_shards_batch` apply `refresh_if_stale:true` once across the selected shard roots instead of refreshing per query. The JSON-lines `search_batch` tool accepts `repo`, `index`, or `index_dir` for the same target-aware plain result shape as `search`; the CLI mirrors this as `search-batch --repo`, `search-batch --index`, or `search-batch --index-dir`. The indexed and shard-specific batch tools remain available for explicit adapters.
- `search_plan`, `indexed_query_plan`, or `shard_query_plan` when a search returns empty or suspicious results and the agent needs missing terms plus retry hints. JSON-lines `search_plan` accepts `repo`, `index`, or `index_dir` for target-aware diagnostics; explicit `search_query_plan`, `indexed_query_plan`, and `shard_query_plan` remain available for adapters that prefer surface-specific tools. Plans include top-level `summary`, `next_action`, `primary_retry_request`, and ready-to-send `retry_requests` when a repair hint has a suggested query; plan batch items include top-level `summary` and promote the same `next_action` at the item level. The CLI mirrors this as `search-plan --repo`, `search-plan --index`, or `search-plan --index-dir`; explicit `index-plan` and `shard-plan` remain available.

CLI-style JSON-lines aliases are accepted for the most guessable names:
`indexed_search` for `indexed_search_code`, `index_plan` for
`indexed_query_plan`, and `shard_plan` for `shard_query_plan`. The plain
JSON-lines `search`, `search_batch`, `search_plan`, and `search_plan_batch`
tools are forgiving targeted entrypoints: pass `repo`, `index`, or `index_dir`
and they use the matching live, indexed, or shard surface.
Search-family CLI commands always emit JSON and accept `--format json`, so
generic wrappers can pass an explicit output contract without special-casing
Orient. JSON-emitting setup and discovery commands such as `discover-repos`,
`index`, `refresh-index`, `ensure-index`, `index-status`, `index-shards`,
`refresh-shards`, `ensure-shards`, `shard-status`, `tool-manifest`,
`mcp-manifest`, and `agent-guide` accept the same explicit `--format json`
contract.
The CLI equivalent for automatic target selection is `orient search-auto`. When
no target flag is supplied, it first tries the shared TCP daemon at
`127.0.0.1:8796`, infers the current git checkout as `repo_filter` when
available, then searches the current directory as a live repo if no daemon is
reachable. Use `--daemon-addr` for another TCP daemon or `--no-daemon` to force
current-directory fallback. `orient search-auto-batch` follows the same
daemon-first behavior. Use `--retry-if-empty` or JSON-lines
`retry_if_empty:true` for first-pass agent searches where one promoted repaired
retry is preferable to manually parsing an empty result.
`orient client-jsonl` automatically adds the shell's current working directory
to no-target search, map, plan, symbol, read, and related-file requests. Other
protocol clients should pass `cwd` explicitly so a shared shard daemon scopes
results to the active checkout. Explicit `repo`, `index`, `index_dir`, or
`repo_filter` arguments still win. Returned follow-up requests already include
an explicit target.
For manual context reads, add `"scope":"symbol"` to `read_range` or to a
`read_ranges` request or range entry when the agent has a line inside a
function, class, or type and wants the window anchored at that definition.
The plain CLI `orient search` command also accepts `--index` and `--index-dir`
as convenience target flags for agents that reach first for `search` and then
add the available search surface.

Query strings support filters such as `repo:service`, `branch:feature/auth`, `origin:example/service`, `path:src/auth` or `dir:src/auth` / `folder:src/auth`, `file:auth.rs` or `filename:auth.rs`, `file:*.rs`, `path:src/*gateway.rs`, `path:src\auth.rs`, `line:42` or `target_line:42`, `lang:rust` or shorthand `lang:rs` / `lang:ts` / `lang:py`, `ext:rs`, `symbol:SessionManager`, `kind:function`, `type:function`, shorthand symbol-kind filters like `fn:issue_token` or `class:SessionManager`, `dep:react`, `import:crate::auth`, `test:false`, `is:test`, `is:source`, `code:true`, `code:false`, `is:code`, `is:docs`, `generated:false`, `is:generated`, positive content aliases like `content:"issue token"` or `text:gateway`, negative filters like `-path:docs`, `-file:*test.rs`, `-folder:vendor`, `-is:generated`, `-lang:md`, `-branch:wip`, `-origin:legacy`, `-kind:class`, `-dep:legacy`, or `-import:old_api`, bare negative content terms like `-deprecated`, and quoted phrases like `"issue token"`. Multi-token queries use AND behavior by default; use `mode:any` in the query or `any_terms:true` in JSON-lines calls for broad orientation searches. Indexed search plans `symbol:` and `kind:` filters through symbol postings and also treats identifier-shaped raw terms such as `SessionManager` and `agent_instructions` as symbol planning hints when a matching symbol exists, while ordinary spaced concept queries stay broad.
`symbol:` filters accept exact names and strong multi-token identifier fragments: `symbol:query_match` can match `symbol_query_match_score`, and `symbol:primary_retry_result` can match `search_auto_primary_retry_result`. Single generic tokens stay exact, so `symbol:path` does not match every `lower_path` or `path_filter` helper unless a symbol named `path` exists.
Bare single-token filename and path-like queries such as `Cargo.toml`, `README.md`, or `src/lib.rs` are inferred as `file:` / `path:` filters so agents that type the file they want get the file, not references to its name. Use `content:Cargo.toml`, `text:README.md`, or `term:src/lib.rs` when the literal string is the target.
Bare pasted locations such as `src/lib.rs:42`, `src/lib.rs:42:9`,
`src/lib.rs#L42-L45`, copied lines such as `src/lib.rs:42: pub fn issue_token`,
Markdown-style file links, common hosted code links, and stack-frame forms such
as `at issueToken (src/lib.rs:42:9)` strip the line/column prefix for matching
and anchor the returned snippet near the line. Absolute pasted paths are
normalized when they are inside the selected repo or index root.
Hosted links may carry fragment or query-string line anchors.
Use `content:` / `text:` / `term:` when an identifier-shaped string should stay a content lookup instead of narrowing indexed search through implicit symbol postings.
Positive non-code language scopes such as `lang:md` keep identifier-shaped terms as content searches instead of requiring symbol postings, so docs/prose lookups stay consistent with live fallback search.
The same applies when positive `file:`, `path:`, or `ext:` scopes clearly target non-code files, such as `path:docs/*.md SessionManager` or `ext:md agent_instructions`.
Broad or negative scopes such as `test:false`, `-ext:rs`, and `-path:tests` also keep identifier-shaped terms as content candidates unless `symbol:` is explicit, so exclusions do not accidentally discard docs or prose matches before filtering.
CLI query positionals accept leading negative filters directly, so `orient search --repo . "-lang:md daemon status"` works without a `--` separator.
`test:true` / `is:test` matches common test layouts across languages, including `tests/`, `__tests__/`, `spec/`, `_test.go`, `_test.rs`, `.test.tsx`, and `.spec.ts`; `test:false` / `is:source` excludes those paths so agents can jump between production code and nearby tests without hand-written path filters.

`code:true` / `is:code` matches implementation source-code languages such as Rust, Python, TypeScript, JavaScript, Go, Ruby, Java, Kotlin, and Swift while excluding docs/config formats like Markdown, TOML, JSON, YAML, and text; `code:false` / `is:docs` does the inverse for prose/config searches.

Generated paths are searchable but demoted by default, including common
generated-code paths and hashed JavaScript bundles under `assets/` or `static/`.
Use `generated:true` / `is:generated` to intentionally inspect generated
output, or `generated:false` / `-is:generated` to exclude it. Generated matching
covers patterns such as `generated/`, `__generated__/`, `codegen/`,
`.generated.*`, `.gen.*`, `.pb.go`, `.g.dart`, `.min.js`, `.bundle.js`, and
`chunk-*.js`.
Live fallback search pushes safe `file:`, `path:`, `ext:`, `lang:`, `test:`, `-file:`, and `-path:` scopes into `rg` globs first, then rechecks every candidate with Orient's query matcher. This keeps scoped searches fast on large workspaces without making the glob layer authoritative.

Use `content:` / `text:` / `term:` when a word or quoted phrase must match file
contents rather than path-like filters. Use `-content:` / `-text:` / `-term:`
to drop files containing noisy boilerplate such as generated markers while
keeping the rest of the query intact.

Structured JSON arguments accept the same common aliases for wrapper authors:
`lang`/`language`, `ext`/`extension`, `kind`/`type`/`symbol_kind`,
`dep`/`deps`/`dependency`, and `module`/`import`/`use`, plus matching
`filename` / `file_name`, `directory` / `folder`, normalized language shorthands such as `rs`, `ts`, `py`, `js`, and `md`, and `exclude_*` aliases such as `exclude_folder`, `exclude_lang`, `exclude_ext`, `exclude_kind`,
`exclude_dep`, `exclude_module`, and `exclude_content`. Symbol-kind values normalize common
singular/plural forms such as `function`/`functions`, `class`/`classes`, and
`interface`/`interfaces`.
CLI flags accept the same common aliases, including `--lang`, `--ext`,
`--type`, `--exclude-lang`, `--exclude-ext`, and `--exclude-type`.

Search results include:

- `path`: the repo-relative, index-relative, or shard-prefixed path.
- `snippet`: line-numbered context.
- `duplicate_group`: present when exact-content hits from repeated worktrees or
  copied files were collapsed into one canonical result.
- `line_range`: displayed snippet bounds.
- `match_lines`: exact hit lines when available.
- `read_range`: a ready-to-pass `{path,start,lines}` follow-up range.
- `read_request`: a ready-to-send JSON-lines request body with the correct read tool and target arguments for the search surface. Generated single-read follow-ups pass the result's `read_range` as `arguments.range`, so adapters can reuse the same range object shape for one-off or batch reads. They also include a `cli` string with the equivalent bounded `orient read-*` command for terminal-native agents and wrappers.
- `related_request`: a ready-to-send JSON-lines request body for nearby source/test files using the matching live, indexed, or shard related-file tool. Generated related follow-ups preserve structured search scopes such as `lang`, `ext`, `test`, `generated`, symbol/kind filters, and `exclude_*` filters while keeping `path` as the result anchor. They include `jsonl`, `client_cli`, and a `cli` string with the equivalent `orient related*` command. Search-generated related-file requests set `include_read_batch:true`, so responses include bounded `read_request` entries plus a top-level `read_batch_request` and `next_action` for reading the returned files in one follow-up.
- `related_symbols_request`: a ready-to-send JSON-lines request body for nearby definitions/types using the matching live, indexed, or shard related-symbol tool. Generated related-symbol follow-ups include a `cli` string with the equivalent `orient related-symbols*` command. Search-generated requests include the original search-language `query`; related-symbol ranking parses it the same way search does, so filters such as `repo:` and `path:` scope the request without becoming noisy symbol terms. Shard related-symbol lookups strip shard-selection filters such as `repo:`, `branch:`, and `origin:` after resolving the hit path, so alias-scoped searches can still open nearby definitions inside the selected shard. Search-generated related-symbol requests set `include_read_batch:true`, so responses include their own bounded `read_request` entries plus a top-level `read_batch_request` and `next_action` for reading returned definitions in one follow-up.
- `context`: optional attached file context when `context_lines` is set.
- `explanation` and `query_plan` when `explain` is set.

`search_auto`, each `search_auto_batch` item, and explicit search batch items
include follow-ups for the chosen live, indexed, or shard surface:

- `next_action`: the best immediate follow-up as
  `{kind,source,summary,request}`. Wrappers can run this first before
  inspecting the rest of the response. Search batch items use read actions
  when hits exist and query-plan actions when an item is empty.
- `next_read_batch_request`: on automatic searches, the preferred read
  follow-up, using normal hits first and retry hits when the original result set
  was empty.
- `query_plan_request`: a ready-to-send plan request for empty, noisy, or
  suspicious results.
- `repo_map_request`: a target-aware map request for entrypoints, tests,
  commands, and top symbols before editing.
- `refresh_request`: present on stale scoped indexed/shard searches. It sets
  `refresh_if_stale:true` and repeats the same automatic search. The same
  request is also nested at `freshness.refresh_request` for compatibility.
- `primary_retry_request`: the promoted repaired search when diagnostics find a
  concrete retry.

Shard searches use bounded parallel fanout. The default cap is 8 shard workers
per query; set `ORIENT_MAX_SHARD_WORKERS=N` on the daemon or CLI process when a
shared machine needs more or less concurrency. `daemon_status` reports the active
`max_shard_workers` value so wrappers can see the local budget without probing.

Generated follow-ups include `jsonl`, `client_cli`, and compact CLI hints.
Batch read follow-ups include `read_budget.grouped_duplicate_count` when the
returned canonical result ranges represent additional duplicate paths.
Set `retry_if_empty:true` or pass `--retry-if-empty` to run the promoted retry
once and receive `primary_retry_result`; it includes compact `summary` fields
with retry hit count, top paths, and score bounds. If that retry returns hits,
Orient also returns a target-aware read batch request for those hits. Set `diagnose:true`
when results are noisy or suspicious to include inline diagnostics even when the
search already returned hits. Batch search items from `search_batch`,
`indexed_search_batch`, and `search_shards_batch` include the same plan/map
follow-up shape plus `read_batch_request`; `next_action` points at the read
when matches are present and at `query_plan_request` when the item is empty.
Repo-map responses include a top-level
`read_batch_request` covering source entrypoints, top symbol definitions, tests,
manifests, important files, and related context in that order. Like single read
requests, generated batch read requests include compact `path:start:lines`
arguments.

Explicit `symbol:` searches center snippets and read ranges on the matching definition line when the language extractor can identify it, even if earlier callers also match the same tokens.

Search `limit` values must be positive and stay under `limit.maximum`; `context_lines`, read ranges, and non-empty batch arrays are bounded by the manifest too, so broad requests fail fast instead of expanding silently.

## Read Next

For most agents, the handoff is:

1. Call search.
2. Collect one or more `read_range` objects from results.
3. Prefer `next_action.request` when present; otherwise send `next_read_batch_request`, `read_batch_request`, one object or an array of `read_range` objects directly to the matching batch read tool, or a result's `read_request` when the wrapper wants a single ready-made follow-up call.
4. Use `related_request` when the likely next step is finding nearby tests, source counterparts, or sibling files for a hit; search-generated related requests already ask Orient to include one batch read request for all returned ranges.
5. Use `related_symbols_request` when the likely next step is finding nearby definitions, types, or other symbols for a hit; search-generated requests already carry the original query and ask Orient to include one batch read request for all returned definitions.

Read-range tools accept `/` or `\` separators in repo-relative paths and reject parent-directory escapes after separator normalization. Shard range and related-context tools accept exact shard-prefixed paths from search hits, such as `service/src/auth.rs`, and also accept unqualified paths like `src/auth.rs` when they resolve to exactly one shard. Ambiguous unqualified paths fail with a prompt to use `<repo>/<path>`.

`read_range` / `open_range` and `read_ranges` / `open_ranges` are target-aware convenience tools: pass `repo`, `index`, or `index_dir` to read from a live repository, persistent index, or shard directory with one adapter path. With no explicit target, protocol clients can pass `cwd` to resolve repository-relative paths inside the active checkout. The single-read `path` and batch `ranges` entries accept copied locations such as `path:line`, `path:start-end`, `path:line: copied text`, `path:start-end: copied text`, `path#Lstart-Lend`, Markdown links, and common hosted code links with fragment or query-string line anchors; `read_ranges.ranges` can be one compact string, one `{path,start,lines}` object, or an array mixing both. Single reads may also pass `range` as a compact string or object with the same shape as a search result's `read_range`. Range objects accept `start_line`, `end_line`, and `line_count` aliases, so adapters can feed search-result `line_range` shaped data back into read tools without translating it first. Batch reads dedupe identical entries and merge overlapping exact ranges before enforcing the total-line budget; symbol-scoped ranges stay separate because they resolve around nearby definitions. Batch reads are capped by range count and total requested lines, so large inspections should be split into smaller follow-up calls. The explicit `read_index_range` and `read_shard_range` families remain available for wrappers that want surface-specific tools.

`related_files` and `related_symbols` are target-aware too: pass `repo`, `index`, or `index_dir` to get nearby files or definitions from the same target style as `search`, or pass `cwd` when using the shared daemon without an explicit target. The CLI mirrors this as `related --repo`, `related --index`, or `related --index-dir`, and likewise for `related-symbols`; pass `--format json` for explicit wrapper contracts and `--include-read-batch` for the wrapped `{summary, results, read_batch_request, next_action}` shape. Related-file and related-symbol tools accept the same structured scoping filters as search, with `path` reserved for the anchor file; use fields such as `test`, `generated`, `lang`, `file`, `exclude_path`, or `exclude_content` to control returned neighbors. Protocol clients can pass `include_read_batch:true` to receive `summary` with `status`, `result_count`, top paths, and score bounds, plus `results`, `read_batch_request`, and `next_action` instead of the default result array. For shard `related_symbols`, include the search-hit `path` so Orient can keep the lookup inside the right shard or alias scope. The explicit `related_index_*` and `related_shard_*` tools remain available.

Examples:

```json
{"id":"read-one","tool":"read_ranges","arguments":{"index":"/path/to/local/cache/orient.index","ranges":{"path":"src/auth.rs","start":1,"lines":80}}}
{"id":"read","tool":"read_ranges","arguments":{"index":"/path/to/local/cache/orient.index","ranges":[{"path":"src/auth.rs","start":1,"lines":80}]}}
{"id":"open-copied","tool":"open_range","arguments":{"repo":"/path/to/repo","path":"src/auth.rs#L40-L45"}}
{"id":"open-shards","tool":"open_ranges","arguments":{"index_dir":"/path/to/local/cache/orient-shards","ranges":[{"path":"service/src/auth.rs","start":40,"lines":80},"service/src/lib.rs#L40-L45"]}}
```

CLI equivalents support repeatable `--range path:start:lines`:

```bash
export ORIENT_INDEX=/path/to/local/cache/orient.index
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient open-index-ranges --index "$ORIENT_INDEX" --range src/auth.rs:1:80
orient open-shard-ranges --index-dir "$ORIENT_SHARDS" --range service/src/auth.rs:40:80
```

Range reads include a compact `summary` with `status`, `line_count`, and `total_lines` alongside the line-numbered `text`, so adapters can cheaply confirm how much context was opened before deciding whether to widen or split the follow-up. Range reads follow manifest bounds: `start >= 1`, `1 <= lines <= lines.maximum`, non-empty batch arrays, and `ranges.maxItems`, so a mistaken request cannot dump unbounded file content.
For CLI adapters, `read-range` / `open-range` and positional `read-ranges` / `open-ranges` entries accept `--format json`, compact `path:start:lines` specs, and copied locations such as `path:line`, `path:start-end`, `path:line: copied text`, `path:start-end: copied text`, and `path#Lstart-Lend`. They also accept `--path`, `--start`, and `--lines`; add a trailing `:symbol` or `:exact` when one range in a batch needs its own scope. Protocol batch strings use the same compact `path:start:lines[:scope]` form. Parsing splits from the right so paths containing `:` still work when the trailing range fields are present.

## Orientation And Repair

Use target-aware `repo_map` before editing unfamiliar code; pass `repo`, `index`, or `index_dir` for live, indexed, or shard orientation. The CLI mirrors this as `repo-map --repo`, `repo-map --index`, or `repo-map --index-dir`; `--format json` is accepted for wrappers that want an explicit output contract, and `--repo-filter` narrows shard maps from the generic command. The explicit `indexed_repo_map` and `shard_repo_map` tools remain available. Maps return compact `summary` counts, top-level entrypoints, manifests, tests, important files, known commands, command hints, dependency hints, import/module hints, top symbols, related files/symbols, a bounded `read_batch_request` for the map's most actionable files and definitions, and `next_action` pointing at that read batch when one is available; the nested `brief` keeps the same summary fields for existing clients. Command hints include common Cargo, Bazel, Python, JavaScript, Maven, Gradle, Go, Swift, Makefile, and Justfile test/build/check/lint/format commands when their manifests or task files are present; scoped shard maps preserve package/task script hints for the selected nested repo alias. The default `detail:"compact"` keeps first-orientation payloads small; request `detail:"full"` only when the agent needs every available import/module hint. The bundled `read_batch_request` defaults to `read_limit:16` ranges and caps at 64, so agents can keep first reads cheap and widen intentionally.

Use target-aware `find_symbol` when the next step is a direct definition jump; pass `repo`, `index`, or `index_dir` for live, indexed, or shard lookup. Use `find_symbol_batch` when the agent has several candidate names from a search or repo map. The explicit `find_index_symbol` and `find_shard_symbol` tools remain available. CLI symbol commands accept `--format json` for wrappers that want an explicit output contract. Symbol hits include flat `name`, `kind`, `path`, and `line` fields plus a ready-to-send bounded `read_request`, so the agent can open definition context without constructing a second request by hand. Single-symbol tools also accept `include_read_batch:true`; when set, they return `summary` with `status`, `symbol_count`, top paths, and symbol kinds, plus `results`, `read_batch_request`, and `next_action` so agents can cheaply spot misses or open all matching definitions in one bounded follow-up. Symbol batch items include `summary` with the same shape; matched items also include `read_batch_request` and `next_action`, so agents can open all candidate definitions for one requested symbol name in one bounded follow-up while cheaply spotting misses. These tools accept the same path, file, language, extension, test, dependency/import, symbol, and kind filters as search, so agents can keep a narrowed scope instead of broadening a symbol lookup across the whole repo set.

For empty or surprising results, call target-aware `search_plan` / `search_plan_batch`, or call `search_query_plan`, `indexed_query_plan`, `shard_query_plan`, their aliases `index_plan` / `shard_plan`, or their batch forms when the adapter already knows the exact surface. The live `search_query_plan` path builds a transient in-memory index for diagnostics without saving anything; persistent index and shard plans use existing index files. Plans include a top-level compact `summary` for wrappers, plus a `diagnosis` object with `status`, `summary`, `next_action`, `primary_hint_kind`, `primary_hint_action`, and the primary suggested retry query when one exists, followed by active filters with candidate match/rejection counts and separate missing postings, filter rejections, phrase/scoring rejections, and final AND/symbol rejections. When a concrete retry exists, plans promote it to `primary_retry_request`, mirror it into `summary.primary_retry_request`, and wrap it in structured `next_action` while keeping the full `retry_requests` list for adapters that want alternatives. Shard plan arrays mirror each per-shard `plan.summary` and `plan.next_action` onto the shard item as top-level `summary` and `next_action`, so wrappers can scan shard diagnostics without descending into every plan. Shard plans also return a `__shard_selection__` diagnostic when `repo:`, `branch:`, `origin:`, or exclusions select no shard, including candidate shard count, active shard filters, and a retry request that drops the over-narrow shard filter. Each `repair_hints` entry has a precise `kind` plus a compact `action` label such as `narrow`, `replace_filter`, `relax_filter`, `relax_query`, `drop_terms`, `broaden_terms`, `shorten_literal`, `broaden_query`, or `inspect`, so adapters can route hints without parsing prose. When a repair hint has a `suggested_query`, the plan also includes a matching `retry_requests` entry for `search`, `search_code`, `indexed_search_code`, or `search_shards` depending on the diagnostic tool used; hints without a suggested query are diagnostic only, so candidate-cap warnings do not create no-op retry loops. Broad indexed searches can return safe `narrow_by_path`, `narrow_by_extension`, `narrow_by_language`, `narrow_by_test`, `narrow_by_generated`, or `narrow_by_code` hints based on the actual candidate set, both when the candidate cap is hit and when a successful query is still noisy enough to benefit from narrowing. Broad shard plans can similarly return `narrow_by_repo`, `narrow_by_branch`, or `narrow_by_origin` hints based on the weighted matching shard set. Facet hints include retry queries only when the facet meaningfully reduces the candidate set. If one scope rejects every candidate, hints such as `relax_path_filter`, `relax_language_filter`, `relax_extension_filter`, `relax_branch_filter`, or `relax_origin_filter` retry without just that filter while preserving the rest of the scope. Filter-only misses can emit targeted `relax_file_filter`, `relax_path_filter`, `relax_dependency_filter`, or similar hints too; those retry requests may use `query:""` while keeping the surviving structured filters. Obvious file and path typos such as `file:athu.rs` or `path:src/ath.rs` can get `replace_file_filter` / `replace_path_filter` retries that drop the stale structured scope and use the nearest indexed file or path, preserving indexed path casing. If an agent accidentally puts a path in `file:`, such as `file:src/ath.rs`, the retry can switch to `path:src/auth.rs`; intentional glob scopes such as `file:*_test.rs` are not fuzzy-replaced. Obvious symbol-kind typos such as `kind:functoin` get a `replace_symbol_kind_filter` retry like `kind:function`; exact symbol typos such as `symbol:SessionManger` get a `replace_symbol_filter` retry like `symbol:SessionManager` when the indexed symbol table has a near match. When strict AND terms all exist but never meet in one file, plans return a `try_any_terms` hint with a `mode:any ...` retry query for broad orientation.
