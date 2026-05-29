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
Agents and wrappers that want a compact first-use recipe can call `agent_guide` or run `orient agent-guide`; it returns install, shard bootstrap, daemon, client, status, one-shot search, local-rule commands, request templates, and follow-up guidance. For copyable local rule files, call `agent_instructions` or run `orient agent-instructions`; it emits a compact local-agent instruction snippet. Both accept `profile:"codex"`, `profile:"claude"`, `profile:"amp"`, or `profile:"generic"` to tailor the rule-placement hint without changing the search tools.

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
{"id":"instructions","tool":"agent_instructions","arguments":{"index_dir":"/path/to/local/cache/orient-shards","profile":"codex"}}
{"id":"guide","tool":"agent_guide","arguments":{"index_dir":"/path/to/local/cache/orient-shards","profile":"codex"}}
```

For an existing shard directory, call `register_shards` to cache only the
manifest. Call `warm_shards` only when every shard index should be loaded
immediately.

`daemon_status`, or the direct CLI wrapper `orient daemon-status`, reports the
daemon's warmed indexes, registered shard directories, and `max_cached_indexes`
cap. The default CLI output is compact; use `orient daemon-status --format json`
for registered-target details, `search_auto_default`, and copyable
`default_requests`.

When exactly one index is warmed or one shard directory is registered, indexed and shard tools
marked with `daemon_default.source` may omit `index` or `index_dir`; otherwise
pass the target explicitly. `search_auto` and `search_auto_batch` use an
explicit `index_dir`, `index`, or `repo` first, then one registered shard directory or warmed index,
then live fallback search from the daemon runtime.

Generated follow-up objects such as `read_request`, `read_batch_request`, `related_request`, `related_symbols_request`, `repo_map_request`, `query_plan_request`, and query-plan `retry_requests` are complete tool requests. They include an `id`, `tool`, `arguments`, raw `jsonl`, a shell-native `client_cli` pipe for `orient client-jsonl`, and, when there is a compact human CLI equivalent, a `cli` hint.

Use `index_status` or `shard_status` when live files may have changed since
indexing. They report added, changed, and deleted files so an agent can refresh
before trusting indexed results. `indexed_search_code` and `search_shards` also
accept `refresh_if_stale:true` for a one-call freshness check and refresh before
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
sets. See [Memory and footprint](memory-footprint.md) for the disk/memory
tradeoffs behind those counters.

Use `ensure_shards` for shard directories shared by several local agents. The lower-level `index_shards` rebuild path refuses to overwrite an existing shard directory when the requested repo set would remove existing shards; pass `force:true` or `orient index-shards --force` only when intentionally replacing that directory.

## Search First

Use the fastest surface that matches your setup:

- `search_auto` when a daemon has exactly one registered shard directory or warmed index, when the request supplies `index_dir`, `index`, or a live `repo`, or when the daemon was started from the desired repo directory. It returns `{query,surface,target,query_plan_request,repo_map_request,results}` and keeps result follow-up requests aligned with the chosen surface.
- `search_code` for a live repo without a prebuilt index.
- `indexed_search_code` for one persistent repo index.
- `search_shards` for a multi-repo shard directory.
- `search_auto_batch`, `search_batch`, `indexed_search_batch`, or `search_shards_batch` when an agent wants to try several query formulations in one round trip. On the warmed-shard path, `search_auto_batch` applies `refresh_if_stale:true` once across the selected shard roots instead of refreshing per query. The JSON-lines `search_batch` tool accepts `repo`, `index`, or `index_dir` for the same target-aware plain result shape as `search`; the CLI mirrors this as `search-batch --repo`, `search-batch --index`, or `search-batch --index-dir`. The indexed and shard-specific batch tools remain available for explicit adapters.
- `search_plan`, `indexed_query_plan`, or `shard_query_plan` when a search returns empty or suspicious results and the agent needs missing terms plus retry hints. JSON-lines `search_plan` accepts `repo`, `index`, or `index_dir` for target-aware diagnostics; explicit `search_query_plan`, `indexed_query_plan`, and `shard_query_plan` remain available for adapters that prefer surface-specific tools. Plans include ready-to-send `retry_requests` when a repair hint has a suggested query. The CLI mirrors this as `search-plan --repo`, `search-plan --index`, or `search-plan --index-dir`; explicit `index-plan` and `shard-plan` remain available.

CLI-style JSON-lines aliases are accepted for the most guessable names:
`indexed_search` for `indexed_search_code`, `index_plan` for
`indexed_query_plan`, and `shard_plan` for `shard_query_plan`. The plain
JSON-lines `search`, `search_batch`, `search_plan`, and `search_plan_batch`
tools are forgiving targeted entrypoints: pass `repo`, `index`, or `index_dir`
and they use the matching live, indexed, or shard surface.
The CLI equivalent for automatic target selection is `orient search-auto`. When
no target flag is supplied, it first tries the shared TCP daemon at
`127.0.0.1:8796`, infers the current git checkout as `repo_filter` when
available, then searches the current directory as a live repo if no daemon is
reachable. Use `--daemon-addr` for another TCP daemon or `--no-daemon` to force
current-directory fallback. `orient search-auto-batch` follows the same
daemon-first rule.
Protocol clients should pass `cwd` on no-target search, map, plan, symbol, read,
and related-file requests so a shared shard daemon scopes results to the active
checkout. Explicit `repo`, `index`, `index_dir`, or `repo_filter` arguments still
win. Returned follow-up requests already include an explicit target.
For manual context reads, add `"scope":"symbol"` to `read_range` or to a
`read_ranges` request or range entry when the agent has a line inside a
function, class, or type and wants the window anchored at that definition.
The plain CLI `orient search` command also accepts `--index` and `--index-dir`
as convenience target flags for agents that reach first for `search` and then
add the available search surface.

Query strings support filters such as `repo:service`, `branch:feature/auth`, `origin:example/service`, `path:src/auth` or `dir:src/auth` / `folder:src/auth`, `file:auth.rs` or `filename:auth.rs`, `file:*.rs`, `path:src/*gateway.rs`, `path:src\auth.rs`, `lang:rust` or shorthand `lang:rs` / `lang:ts` / `lang:py`, `ext:rs`, `symbol:SessionManager`, `kind:function`, `type:function`, `dep:react`, `import:crate::auth`, `test:false`, `is:test`, `is:source`, `code:true`, `code:false`, `is:code`, `is:docs`, `generated:false`, `is:generated`, positive content aliases like `content:"issue token"` or `text:gateway`, negative filters like `-path:docs`, `-file:*test.rs`, `-folder:vendor`, `-is:generated`, `-lang:md`, `-branch:wip`, `-origin:legacy`, `-kind:class`, `-dep:legacy`, or `-import:old_api`, and quoted phrases like `"issue token"`. Multi-token queries use AND behavior by default; use `mode:any` in the query or `any_terms:true` in JSON-lines calls for broad orientation searches. Indexed search plans `symbol:` and `kind:` filters through symbol postings and also treats identifier-shaped raw terms such as `SessionManager` and `agent_instructions` as symbol planning hints when a matching symbol exists, while ordinary spaced concept queries stay broad.
`symbol:` filters accept exact names and strong multi-token identifier fragments: `symbol:query_match` can match `symbol_query_match_score`, and `symbol:primary_retry_result` can match `search_auto_primary_retry_result`. Single generic tokens stay exact, so `symbol:path` does not match every `lower_path` or `path_filter` helper unless a symbol named `path` exists.
Bare single-token filename and path-like queries such as `Cargo.toml`, `README.md`, or `src/lib.rs` are inferred as `file:` / `path:` filters so agents that type the file they want get the file, not references to its name. Use `content:Cargo.toml`, `text:README.md`, or `term:src/lib.rs` when the literal string is the target.
Bare pasted locations such as `src/lib.rs:42`, `src/lib.rs:42:9`,
`src/lib.rs#L42-L45`, copied lines such as `src/lib.rs:42: pub fn issue_token`,
and stack-frame forms such as `at issueToken (src/lib.rs:42:9)` strip the
line/column prefix for matching and anchor the returned snippet near the line.
Absolute pasted paths are normalized when they are inside the selected repo or
index root.
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
- `line_range`: displayed snippet bounds.
- `match_lines`: exact hit lines when available.
- `read_range`: a ready-to-pass `{path,start,lines}` follow-up range.
- `read_request`: a ready-to-send JSON-lines request body with the correct read tool and target arguments for the search surface. Generated read follow-ups also include a `cli` string with the equivalent bounded `orient read-*` command for terminal-native agents and wrappers.
- `related_request`: a ready-to-send JSON-lines request body for nearby source/test files using the matching live, indexed, or shard related-file tool. Generated related follow-ups preserve structured search scopes such as `lang`, `ext`, `test`, `generated`, symbol/kind filters, and `exclude_*` filters while keeping `path` as the result anchor. They include `jsonl`, `client_cli`, and a `cli` string with the equivalent `orient related*` command. Related-file responses include bounded `read_request` entries for opening each returned file directly.
- `related_symbols_request`: a ready-to-send JSON-lines request body for nearby definitions/types using the matching live, indexed, or shard related-symbol tool. Generated related-symbol follow-ups include a `cli` string with the equivalent `orient related-symbols*` command. Search-generated requests include the original search-language `query`; related-symbol ranking parses it the same way search does, so filters such as `repo:` and `path:` scope the request without becoming noisy symbol terms. Shard related-symbol lookups strip shard-selection filters such as `repo:`, `branch:`, and `origin:` after resolving the hit path, so alias-scoped searches can still open nearby definitions inside the selected shard. Related-symbol responses include their own bounded `read_request` entries for opening definition context directly.
- `context`: optional attached file context when `context_lines` is set.
- `explanation` and `query_plan` when `explain` is set.

`search_auto` and each `search_auto_batch` item also include `query_plan_request`, a ready-to-send plan request for the chosen live, indexed, or shard surface. Generated query-plan follow-ups include `jsonl`, `client_cli`, and a `cli` string with the equivalent `orient search-plan` command. When an automatic search is empty, the response also includes `query_plan_result` with repair hints and retry requests immediately; generated retry requests include `jsonl`, `client_cli`, and `cli` strings with equivalent `orient search` commands. On empty or `diagnose:true` indexed/shard searches, Orient checks the scoped index for freshness without scanning unrelated shards; if it is stale, the response includes `freshness` with changed/added/deleted counts and a `refresh_request` that sets `refresh_if_stale:true` and repeats the same `search_auto` request. When diagnostics are present, `primary_diagnosis` promotes the compact plan diagnosis to the top level; when a concrete retry exists, `primary_retry_request` promotes the first ready-to-send retry too, so wrappers can recover without parsing the full diagnostic plan. Set `retry_if_empty:true` or pass `--retry-if-empty` to run that primary retry once and receive `primary_retry_result` in the same response. Set `diagnose:true` when results are noisy or suspicious to include `query_plan_result`, `primary_diagnosis`, `primary_retry_request`, and stale-index freshness when applicable even with hits, saving a second diagnostic call. They also include a target-aware `repo_map_request` when the agent needs entrypoints, tests, commands, or top symbols before editing; shard map follow-ups preserve `repo:`, `branch:`, `origin:`, and matching exclusions from structured arguments or the query string. Generated map follow-ups include `jsonl`, `client_cli`, and a `cli` string with the equivalent `orient repo-map` command. Batch search items from `search_batch`, `indexed_search_batch`, and `search_shards_batch` include the same `read_batch_request` shape. Repo-map responses include a top-level `read_batch_request` covering entrypoints, manifests, important files, tests, top symbol definitions, and related context. Like single read requests, generated batch read requests include `jsonl`, `client_cli`, and `cli` hints using compact `path:start:lines` arguments.

Explicit `symbol:` searches center snippets and read ranges on the matching definition line when the language extractor can identify it, even if earlier callers also match the same tokens.

Search `limit` values must be positive and stay under `limit.maximum`; `context_lines`, read ranges, and non-empty batch arrays are bounded by the manifest too, so broad requests fail fast instead of expanding silently.

## Read Next

For most agents, the handoff is:

1. Call search.
2. Collect one or more `read_range` objects from results.
3. Send `read_batch_request` when it is present, pass one object or an array of `read_range` objects directly to the matching batch read tool, or send a result's `read_request` when the wrapper wants a single ready-made follow-up call.
4. Use `related_request` when the likely next step is finding nearby tests, source counterparts, or sibling files for a hit; returned related files include `read_request` payloads for opening the file directly.
5. Use `related_symbols_request` when the likely next step is finding nearby definitions, types, or other symbols for a hit; search-generated requests already carry the original query, and returned related symbols include `read_request` payloads for the next bounded read.

Read-range tools accept `/` or `\` separators in repo-relative paths and reject parent-directory escapes after separator normalization. Shard range and related-context tools accept exact shard-prefixed paths from search hits, such as `service/src/auth.rs`, and also accept unqualified paths like `src/auth.rs` when they resolve to exactly one shard. Ambiguous unqualified paths fail with a prompt to use `<repo>/<path>`.

`read_range` / `open_range` and `read_ranges` / `open_ranges` are target-aware convenience tools: pass `repo`, `index`, or `index_dir` to read from a live repository, persistent index, or shard directory with one adapter path. With no explicit target, protocol clients can pass `cwd` to resolve repository-relative paths inside the active checkout. The single-read `path` and batch `ranges` entries accept copied locations such as `path:line`, `path:line: copied text`, and `path#Lstart-Lend`; `read_ranges.ranges` can be one compact string, one `{path,start,lines}` object, or an array mixing both. The explicit `read_index_range` and `read_shard_range` families remain available for wrappers that want surface-specific tools.

`related_files` and `related_symbols` are target-aware too: pass `repo`, `index`, or `index_dir` to get nearby files or definitions from the same target style as `search`, or pass `cwd` when using the shared daemon without an explicit target. The CLI mirrors this as `related --repo`, `related --index`, or `related --index-dir`, and likewise for `related-symbols`. Related-file and related-symbol tools accept the same structured scoping filters as search, with `path` reserved for the anchor file; use fields such as `test`, `generated`, `lang`, `file`, `exclude_path`, or `exclude_content` to control returned neighbors. For shard `related_symbols`, include the search-hit `path` so Orient can keep the lookup inside the right shard or alias scope. The explicit `related_index_*` and `related_shard_*` tools remain available.

Examples:

```json
{"id":"read-one","tool":"read_ranges","arguments":{"index":"/path/to/local/cache/orient.index","ranges":{"path":"src/auth.rs","start":1,"lines":80}}}
{"id":"read","tool":"read_ranges","arguments":{"index":"/path/to/local/cache/orient.index","ranges":[{"path":"src/auth.rs","start":1,"lines":80}]}}
{"id":"read-copied","tool":"read_range","arguments":{"repo":"/path/to/repo","path":"src/auth.rs#L40-L45"}}
{"id":"read-shards","tool":"read_ranges","arguments":{"index_dir":"/path/to/local/cache/orient-shards","ranges":[{"path":"service/src/auth.rs","start":40,"lines":80},"service/src/lib.rs#L40-L45"]}}
```

CLI equivalents support repeatable `--range path:start:lines`:

```bash
export ORIENT_INDEX=/path/to/local/cache/orient.index
export ORIENT_SHARDS=/path/to/local/cache/orient-shards

orient read-index-ranges --index "$ORIENT_INDEX" --range src/auth.rs:1:80
orient read-shard-ranges --index-dir "$ORIENT_SHARDS" --range service/src/auth.rs:40:80
```

Range reads follow manifest bounds: `start >= 1`, `1 <= lines <= lines.maximum`, non-empty batch arrays, and `ranges.maxItems`, so a mistaken request cannot dump unbounded file content.
For CLI adapters, `read-range` and positional `read-ranges` entries accept compact `path:start:lines` specs as well as copied locations such as `path:line`, `path:line: copied text`, and `path#Lstart-Lend`. They also accept `--path`, `--start`, and `--lines`; add a trailing `:symbol` or `:exact` when one range in a batch needs its own scope. Protocol batch strings use the same compact `path:start:lines[:scope]` form. Parsing splits from the right so paths containing `:` still work when the trailing range fields are present.

## Orientation And Repair

Use target-aware `repo_map` before editing unfamiliar code; pass `repo`, `index`, or `index_dir` for live, indexed, or shard orientation. The CLI mirrors this as `repo-map --repo`, `repo-map --index`, or `repo-map --index-dir`; `--format json` is accepted for wrappers that want an explicit output contract, and `--repo-filter` narrows shard maps from the generic command. The explicit `indexed_repo_map` and `shard_repo_map` tools remain available. Maps return entrypoints, manifests, tests, important files, top symbols, related files/symbols, command hints, dependency hints, import/module hints, and a bounded `read_batch_request` for the map's most actionable files and definitions. Command hints include common Cargo, Bazel, Python, JavaScript, Maven, Gradle, Go, Swift, Makefile, and Justfile test/build/check/lint/format commands when their manifests or task files are present. The default `detail:"compact"` keeps first-orientation payloads small; request `detail:"full"` only when the agent needs every available import/module hint. The bundled `read_batch_request` defaults to `read_limit:16` ranges and caps at 64, so agents can keep first reads cheap and widen intentionally.

Use target-aware `find_symbol` when the next step is a direct definition jump; pass `repo`, `index`, or `index_dir` for live, indexed, or shard lookup. Use `find_symbol_batch` when the agent has several candidate names from a search or repo map. The explicit `find_index_symbol` and `find_shard_symbol` tools remain available. Symbol hits include flat `name`, `kind`, `path`, and `line` fields plus a ready-to-send bounded `read_request`, so the agent can open definition context without constructing a second request by hand. Symbol batch items also include `read_batch_request` when the item has hits, so agents can open all candidate definitions for one requested name in one bounded follow-up. These tools accept the same path, file, language, extension, test, dependency/import, symbol, and kind filters as search, so agents can keep a narrowed scope instead of broadening a symbol lookup across the whole repo set.

For empty or surprising results, call target-aware `search_plan` / `search_plan_batch`, or call `search_query_plan`, `indexed_query_plan`, `shard_query_plan`, their aliases `index_plan` / `shard_plan`, or their batch forms when the adapter already knows the exact surface. The live `search_query_plan` path builds a transient in-memory index for diagnostics without saving anything; persistent index and shard plans use existing index files. Plans include a compact `diagnosis` object with `status`, `summary`, `next_action`, `primary_hint_kind`, `primary_hint_action`, and the primary suggested retry query when one exists, followed by active filters with candidate match/rejection counts and separate missing postings, filter rejections, phrase/scoring rejections, and final AND/symbol rejections. Shard plans also return a `__shard_selection__` diagnostic when `repo:`, `branch:`, `origin:`, or exclusions select no shard, including candidate shard count, active shard filters, and a retry request that drops the over-narrow shard filter. Each `repair_hints` entry has a precise `kind` plus a compact `action` label such as `narrow`, `replace_filter`, `relax_filter`, `relax_query`, `drop_terms`, `broaden_terms`, `shorten_literal`, `broaden_query`, or `inspect`, so adapters can route hints without parsing prose. When a repair hint has a `suggested_query`, the plan also includes a matching `retry_requests` entry for `search`, `search_code`, `indexed_search_code`, or `search_shards` depending on the diagnostic tool used; hints without a suggested query are diagnostic only, so candidate-cap warnings do not create no-op retry loops. Broad indexed searches can return safe `narrow_by_path`, `narrow_by_extension`, `narrow_by_language`, `narrow_by_test`, `narrow_by_generated`, or `narrow_by_code` hints based on the actual candidate set, both when the candidate cap is hit and when a successful query is still noisy enough to benefit from narrowing. Broad shard plans can similarly return `narrow_by_repo`, `narrow_by_branch`, or `narrow_by_origin` hints based on the weighted matching shard set. Facet hints include retry queries only when the facet meaningfully reduces the candidate set. If one scope rejects every candidate, hints such as `relax_path_filter`, `relax_language_filter`, `relax_extension_filter`, `relax_branch_filter`, or `relax_origin_filter` retry without just that filter while preserving the rest of the scope. Filter-only misses can emit targeted `relax_file_filter`, `relax_path_filter`, `relax_dependency_filter`, or similar hints too; those retry requests may use `query:""` while keeping the surviving structured filters. Obvious file and path typos such as `file:athu.rs` or `path:src/ath.rs` can get `replace_file_filter` / `replace_path_filter` retries that drop the stale structured scope and use the nearest indexed file or path, preserving indexed path casing. If an agent accidentally puts a path in `file:`, such as `file:src/ath.rs`, the retry can switch to `path:src/auth.rs`; intentional glob scopes such as `file:*_test.rs` are not fuzzy-replaced. Obvious symbol-kind typos such as `kind:functoin` get a `replace_symbol_kind_filter` retry like `kind:function`; exact symbol typos such as `symbol:SessionManger` get a `replace_symbol_filter` retry like `symbol:SessionManager` when the indexed symbol table has a near match. When strict AND terms all exist but never meet in one file, plans return a `try_any_terms` hint with a `mode:any ...` retry query for broad orientation.
