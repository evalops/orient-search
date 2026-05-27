# Orient Search

Rust-native fast local code search for coding agents. It gives Codex, Claude, Amp-style agents, and similar tools a cheap way to answer “where is the relevant thing?” before they burn tool calls on repeated `rg`, `find`, `cat`, and failed path probes.

## What It Does

- Indexes a local repo and returns compact search answers.
- Searches code with a fast `rg`-backed hot path plus an experimental persistent Rust index.
- Boosts exact symbol definitions in both fallback and indexed search.
- Finds symbols plus related test/source files and nearby definitions.
- Reads bounded line ranges after search hits, with line-numbered output.
- Builds repo maps with entrypoints, manifests, tests, top symbols, commands, and important files.
- Infers known commands from repo manifests.
- Discovers local repo roots under broad workspaces for fast shard setup.
- Exposes a Rust CLI and JSON-lines tool server suitable for MCP-style wrapping.

## Rust Quickstart

```bash
cargo build
cargo test

# Brief a repo.
cargo run -- brief --repo /path/to/repo
cargo run -- repo-map --repo /path/to/repo --symbols 50 --tests 50

# Search code.
cargo run -- search --repo /path/to/repo "session token auth"
cargo run -- search --repo /path/to/repo 'symbol:SessionManager lang:rust -path:docs "issue token"' \
  --snippet block \
  --explain

# Build and query a persistent local index.
cargo run -- index --repo /path/to/repo --output /tmp/orient.index
cargo run -- refresh-index --repo /path/to/repo --index /tmp/orient.index
cargo run -- indexed-search --index /tmp/orient.index "session token auth" \
  --path src/ \
  --language rust \
  --extension rs \
  --require-all \
  --snippet symbol
cargo run -- index-map --index /tmp/orient.index --symbols 50 --tests 50
cargo run -- read-index-range --index /tmp/orient.index src/auth.rs --start 40 --lines 80

# Build and search a local multi-repo shard directory.
cargo run -- discover-repos --root /Users/jonathanhaas/Documents/Projects --max-depth 4 --limit 200
cargo run -- index-shards \
  --discover-root /Users/jonathanhaas/Documents/Projects \
  --discover-root /Users/jonathanhaas/repos \
  --max-depth 4 \
  --discover-limit 200 \
  --output-dir /tmp/orient-shards
cargo run -- index-shards \
  --repo /path/to/repo-a \
  --repo /path/to/repo-b \
  --output-dir /tmp/orient-shards
cargo run -- ensure-shards \
  --discover-root /Users/jonathanhaas/Documents/Projects \
  --discover-root /Users/jonathanhaas/repos \
  --max-depth 4 \
  --discover-limit 200 \
  --output-dir /tmp/orient-shards
cargo run -- refresh-shards --index-dir /tmp/orient-shards
cargo run -- shard-map --index-dir /tmp/orient-shards --repo repo-a --symbols 50 --tests 50
cargo run -- search-shards --index-dir /tmp/orient-shards "session token auth"
cargo run -- search-shards --index-dir /tmp/orient-shards "repo:repo-a session token auth"
cargo run -- search-shards --index-dir /tmp/orient-shards "repo:maestro app server"
cargo run -- shard-symbol --index-dir /tmp/orient-shards --repo repo-a SessionManager
cargo run -- read-shard-range --index-dir /tmp/orient-shards repo-a/src/auth.rs --start 40 --lines 80
cargo run -- read-shard-range --index-dir /tmp/orient-shards maestro/src/app.rs --start 40 --lines 80
cargo run -- related-shard --index-dir /tmp/orient-shards maestro/src/app.rs
cargo run -- related-shard-symbols --index-dir /tmp/orient-shards maestro/src/app.rs --query "app server"

# Find a symbol.
cargo run -- symbol --repo /path/to/repo SessionManager
cargo run -- index-symbol --index /tmp/orient.index SessionManager

# Find related tests/files.
cargo run -- related --repo /path/to/repo src/auth.py
cargo run -- related-symbols --repo /path/to/repo --path src/auth.py --query "session token"
cargo run -- related-index --index /tmp/orient.index src/auth.py
cargo run -- related-index-symbols --index /tmp/orient.index --path src/auth.py --query "session token"

# Read a bounded, line-numbered file range.
cargo run -- read-range --repo /path/to/repo src/auth.py --start 40 --lines 80
cargo run -- read-ranges --repo /path/to/repo src/auth.py tests/auth_test.py --start 40 --lines 80
cargo run -- read-index-ranges --index /tmp/orient.index src/auth.py tests/auth_test.py --start 40 --lines 80
cargo run -- read-shard-ranges --index-dir /tmp/orient-shards maestro/src/app.rs maestro/tests/app_test.rs --start 40 --lines 80

# Print the agent tool manifest used by JSON-lines wrappers.
cargo run -- tool-manifest

# Measure p50/p95/max search latency with the same code paths agents use.
cargo run --release -- bench-search \
  --repo /Users/jonathanhaas/Documents/Projects \
  --runs 10 \
  --warmup 3 \
  --fail-p95-ms 300 \
  --write-baseline /tmp/orient-projects-bench.json \
  "session token auth" \
  "browser session implementation" \
  "postgres migration user"

cargo run --release -- bench-search \
  --repo /Users/jonathanhaas/Documents/Projects \
  --runs 10 \
  --warmup 3 \
  --baseline /tmp/orient-projects-bench.json \
  --max-p95-regression 0.25 \
  "session token auth" \
  "browser session implementation" \
  "postgres migration user"
```

## JSON-Lines Server

`orient serve-jsonl` reads one request per line from stdin and writes one response per line to stdout.
`orient serve-tcp` exposes the same protocol over localhost TCP with a shared in-process index cache, which is the better shape when several local agents are searching the same shard directory or persistent indexes. Cached index objects are shared across connections for shard search, repo maps, symbol lookup, range reads, and related-context followups, and request execution does not hold a global daemon lock around searches.

```bash
cargo run -- serve-jsonl
cargo run -- serve-tcp --addr 127.0.0.1:8796
cargo run -- serve-tcp --addr 127.0.0.1:8796 \
  --index /tmp/orient.index \
  --index-dir /tmp/orient-shards
cargo run -- client-jsonl --addr 127.0.0.1:8796
```

Example request:

```json
{"id":1,"tool":"search_code","arguments":{"repo":"/path/to/repo","query":"issue token","limit":5,"extension":"rs","require_all":true,"snippet":"block","explain":true}}
```

Discover and index shard roots:

```json
{"id":2,"tool":"discover_repos","arguments":{"root":"/Users/jonathanhaas/Documents/Projects","max_depth":4,"limit":200}}
{"id":3,"tool":"index_shards","arguments":{"discover_roots":["/Users/jonathanhaas/Documents/Projects","/Users/jonathanhaas/repos"],"max_depth":4,"discover_limit":200,"output_dir":"/tmp/orient-shards"}}
```

Shard request:

```json
{"id":4,"tool":"search_shards","arguments":{"index_dir":"/tmp/orient-shards","query":"repo:billing invoice total","limit":5,"require_all":true,"explain":true}}
```

Batch read request:

```json
{"id":5,"tool":"read_shard_ranges","arguments":{"index_dir":"/tmp/orient-shards","ranges":[{"path":"billing/src/billing.rs","start":1,"lines":40},{"path":"billing/tests/billing_test.rs","start":1,"lines":80}]}}
```

Supported tools:

- `list_tools`
- `tool_manifest`
- `daemon_status`
- `warm_index`
- `warm_shards`
- `discover_repos`
- `repo_brief`
- `repo_map`
- `indexed_repo_map`
- `read_range`
- `read_ranges`
- `search_code`
- `indexed_search_code`
- `read_index_range`
- `read_index_ranges`
- `index_shards`
- `ensure_shards`
- `refresh_shards`
- `search_shards`
- `read_shard_range`
- `read_shard_ranges`
- `shard_repo_map`
- `find_shard_symbol`
- `find_symbol`
- `find_index_symbol`
- `related_files`
- `related_index_files`
- `related_shard_files`
- `related_symbols`
- `related_index_symbols`
- `related_shard_symbols`

`tool_manifest` returns the same tool list with descriptions plus required and optional argument names, so a wrapper can bootstrap the JSON-lines surface without scraping this README.
`daemon_status` reports local warm-cache counts for the current daemon process; it does not inspect Codex/Claude sessions or emit telemetry.
Use `warm_index` or `warm_shards`, or pass `--index` / `--index-dir` to `serve-tcp`, to load persistent indexes before the first agent query.
Use `ensure_shards` when several local agents share the same workspaces: it builds the shard directory on first run, refreshes it on later runs, and warms the daemon cache in one request.
Use `discover_repos`, or `index_shards` with `discover_root`, when a local machine has many duplicated worktrees and nested repo collections.
For indexed or shard JSON search arguments, use `repo` or `repo_filter` to restrict by repository name. Shard search also records aliases for immediate child directories that look like repos, so a shard rooted at a dated worktree can still answer filters like `repo:maestro` or `repo:platform`. Alias-scoped shard search, symbol lookup, repo maps, and related-context tools return alias-prefixed paths such as `maestro/src/app.rs`, and `read-shard-range` accepts those paths directly. For `search_code`, `repo` is the repository root path, so use `repo_filter` for name filtering.

## Query Language

Search queries support agent-friendly filters inline with normal terms:

- `file:auth.rs`: match file basename.
- `path:src/auth`: require a path substring.
- `lang:rust` or `language:rust`: require a detected language.
- `ext:rs`: require a file extension.
- `symbol:SessionManager`: require/boost an exact symbol definition.
- `repo:orient-search`: require the root repo name.
- `test:true` or `test:false`: include only test or non-test paths.
- `-path:docs`, `-file:generated`, `-lang:markdown`, `-ext:md`, `-symbol:Foo`, `-repo:old`: exclude matches.
- `"issue token"`: keep multi-word literals grouped while parsing.

Multiple positive terms use AND behavior by default, so `session token auth` means all three terms should be represented in the returned result.
Path, file, repo, extension, language, and symbol filters are matched case-insensitively, so agents do not need to guess exact repository casing before searching.

## Snippet Modes

Search tools and CLI commands accept `--snippet <mode>` or JSON `"snippet":"<mode>"`:

- `short`: one matching line.
- `medium`: a compact default context window.
- `block`: a larger context block for deciding whether to edit.
- `symbol`: prefer the matching symbol definition when a symbol signal is available.

Indexed search persists line-offset tables, token-to-line tables, and bounded source snapshots in the binary index. `indexed-search`, `read-index-range`, `read-index-ranges`, and shard range reads can render context from the saved snapshot even when the live file has moved or is temporarily unavailable.

Every search result with a numbered snippet includes a `line_range` object with `start_line` and `end_line`, so agents can immediately call `read-range`, `read-index-range`, or `read-shard-range` without scraping snippet text. Results also include `match_lines` when the engine can identify exact query-hit lines inside the file, which lets wrappers jump to the most relevant line after reading the broader range.

## Ranking Explanations

Search commands and JSON-lines tools accept `--explain` or JSON `"explain":true`. Normal output stays compact; explain mode adds an `explanation` array to each result with structured ranking signals such as:

- `path_match`: query token appeared in the path.
- `line_match` or `content_match`: query token appeared in matched content.
- `term_frequency`: indexed term frequency contributed to score.
- `symbol_exact` or `symbol_overlap`: symbol matching contributed to score.

Indexed searches also include a `query_plan` object in explain mode. It reports the planner strategy, normalized query tokens/trigrams, the rarest planned posting lists with posting counts, whether AND semantics are required, and the final candidate count before scoring. This is meant to help agent wrappers debug broad or empty queries without guessing how the index searched.

When repeated worktrees or manifest copies produce equivalent hits, the top result can include a compact `duplicate_group` with a normalized `canonical_path`, a suppressed duplicate count, and up to eight hidden duplicate paths. This keeps result lists short while still showing agents when a match exists in multiple local copies.

## Success Criteria

The build is useful when it can:

- Answer repo brief/search/symbol/related-file questions through Rust CLI and JSON-lines server.
- Let an agent search, inspect a repo map, and read bounded file ranges without shelling out to `cat`/`sed`.
- Let an agent search and read bounded file ranges from a local multi-repo shard directory.
- Return wide-tree search results in hundreds of milliseconds, not seconds.
- Bound the hot path with a wall-clock timeout and match caps so pathological trees cannot hang searches.
- Provide a persistent indexed search mode that can evolve toward Zoekt-style shards/postings.
- Refresh the persistent index incrementally by reusing unchanged and renamed file metadata/postings.
- Establish a baseline for fast local code search so future agent runs can use fewer exploratory commands.
- Pass the Rust test suite.

Product impact criteria for follow-up adoption:

- Fewer exploratory search/read calls in comparable coding sessions.
- Fewer failed path probes and dead-end search commands.
- Faster useful file discovery before first edit.
- No task-quality regression.

Current search baseline:

- `orient bench-search --repo . "indexed search symbol filters"`: `9.413ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "session token auth"`: `19.165ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "browser session implementation"`: `18.178ms` p95 after warmup.
- `orient bench-search --repo /Users/jonathanhaas/Documents/Projects "postgres migration user"`: `37.016ms` p95 after warmup.
- The `rg` hot path has a `250ms` wall-clock timeout plus a bounded match cap; timed-out searches return partial results rather than hanging.
- `orient index --repo . --output /tmp/orient-self.index`: versioned binary index with file metadata, content token postings, path token postings, trigram postings, line offsets, token-to-line tables, bounded source snapshots, and symbol boosts.
- `orient discover-repos --root /Users/jonathanhaas/Documents/Projects --max-depth 4 --limit 200`: finds git or manifest-backed repo roots while skipping dependency/build directories and prioritizing visible canonical repos ahead of dated split, temp, and worktree folders when limits are small.
- `orient index-shards --repo repo-a --repo repo-b --output-dir /tmp/orient-shards`: writes per-repo index shards plus a manifest for local multi-repo search, including stable aliases for nested repo directories.
- `orient index-shards --discover-root /Users/jonathanhaas/Documents/Projects --discover-root /Users/jonathanhaas/repos --output-dir /tmp/orient-shards`: discovers repos from several local workspace roots and writes shard indexes in one step.
- `orient ensure-shards --discover-root /Users/jonathanhaas/Documents/Projects --discover-root /Users/jonathanhaas/repos --output-dir /tmp/orient-shards`: builds missing shard directories or refreshes existing ones, which is the easiest bootstrap for a shared local daemon.
- Search results dedupe repeated worktree copies and include `duplicate_group` metadata when duplicates were suppressed.
- `orient search-shards --index-dir /tmp/orient-shards "repo:maestro app server"`: returns stable alias-prefixed paths that can be passed straight to `read-shard-range`.
- `orient related-shard --index-dir /tmp/orient-shards maestro/src/app.rs`: returns nearby source/test files from the same shard alias scope.
- `orient related-shard-symbols --index-dir /tmp/orient-shards maestro/src/app.rs --query "app server"`: returns nearby definitions from the same shard alias scope.
- `orient related-index` and `orient related-index-symbols`: return nearby files and definitions directly from persisted index metadata, without rebuilding a live repo scan.
- Search results include structured `line_range` and `match_lines` metadata for direct read-range follow-up calls.
- `orient refresh-shards --index-dir /tmp/orient-shards`: refreshes each shard incrementally, reusing unchanged file metadata and postings per repo, and refreshes nested repo aliases.
- `orient refresh-index --repo . --index /tmp/orient-self.index`: reuses unchanged files, detects same-content renames, and refreshes changed/deleted files. Refresh stats include `renamed_files`.
- `orient index-map --index /tmp/orient-self.index`: returns repo-map orientation directly from the persistent index without rebuilding a live repo scan.
- `orient shard-map --index-dir /tmp/orient-shards`: returns repo-prefixed repo maps for local multi-repo shard directories.
- `orient bench-search --repo . --index /tmp/orient-self.index "indexed search symbol filters"`: `0.632ms` p95 after warmup.

Benchmark methodology:

- Use `cargo build --release`, then run `orient bench-search`.
- Warm up each query before collecting samples.
- Report `p95_ms` and `max_ms` from repeated searches, not one-off timings.
- Benchmark the fallback path without `--index`; benchmark the persistent indexed path with `--index /tmp/orient-self.index`.
- Use `--fail-p95-ms <milliseconds>` in CI or local regression checks when you want slow queries to fail the command.
- Use `--write-baseline <path>` to save a benchmark report and `--baseline <path> --max-p95-regression <ratio>` to fail later runs when matching query p95 latency regresses beyond that ratio.

See [docs/fast-search-roadmap.md](docs/fast-search-roadmap.md) for the Zoekt/Sourcegraph/Amp-inspired roadmap.

## Architecture

- `src/fast_index.rs`: experimental persistent content/path token plus trigram index and indexed search.
- `src/shards.rs`: local multi-repo shard manifests and merged shard search.
- `src/repo_index.rs`: repo indexing, symbol extraction, snippet rendering, code search, related-file lookup.
- `src/query.rs`: inline query-language parsing and filter merging.
- `src/server.rs`: JSON-lines tool dispatch and a localhost TCP daemon with a shared concurrent index cache.
- `src/main.rs`: CLI.
