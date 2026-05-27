# Fast Search Roadmap

This project should become a Rust-native, local-agent-first search layer: the useful architecture shape of Zoekt, but optimized for Codex/Claude/Amp-style tool calls instead of a human web UI.

## Research Notes

The GitHub sweep covered 722 code-search, agent-context, semantic-search, and repository-understanding projects. The strongest references:

| Project | Useful signal |
| --- | --- |
| [sourcegraph/zoekt](https://github.com/sourcegraph/zoekt) | Fast trigram code search, rich query language, shards, mmap-friendly index files, branch masks, ranking with symbol/path signals. |
| [Sourcegraph Zoekt blog](https://sourcegraph.com/blog/zoekt-creating-internal-tools-at-google) | Origin story and rationale for internal-code-search tooling that needs to be fast enough to become default behavior. |
| [Zoekt design docs](https://github.com/sourcegraph/zoekt/blob/main/doc/design.md) | Explicit target of sub-50ms search over large corpora, positional trigrams, shard format, query trees, and ranking signals. |
| [probelabs/probe](https://github.com/probelabs/probe) | Agent-oriented search that combines ripgrep speed with tree-sitter/AST-aware snippets. |
| [MinishLab/semble](https://github.com/MinishLab/semble) | Agent-first code search framing: reduce grep/read token burn, local CPU, fast indexing/search. |
| [BloopAI/bloop](https://github.com/BloopAI/bloop) | Rust code-search engine precedent, hybrid regex/semantic direction. |
| [zilliztech/claude-context](https://github.com/zilliztech/claude-context) | MCP-shaped code search for Claude Code with vector DB integration. |
| [colbymchenry/codegraph](https://github.com/colbymchenry/codegraph) | Pre-indexed local graph for coding agents, framed around fewer tokens and fewer tool calls. |
| [lemon07r/Vera](https://github.com/lemon07r/Vera) | Rust local code search with BM25, vector similarity, reranking, tree-sitter metadata. |

## Product Thesis

Agents already search. The win is not convincing them to search; it is making search cheap, low-latency, and structured enough that they stop doing dozens of exploratory `rg`, `find`, `ls`, and `cat` calls.

The Ceramic-level insight for this product is: agents already search, so the leverage is making local code search cheap enough that it becomes the default first action before scattered `rg`, `find`, `ls`, and `cat` exploration.

## Current Baseline

Implemented now:

- `orient search`: fast `rg`-backed candidate collection with Rust-side scoring/snippets.
- Agent-oriented query language for `file:`, `path:`, `lang:`, `ext:`, `symbol:`, `repo:`, `test:`, filter-only discovery queries, separator-normalized exact quoted phrases, negative filters, and default multi-term AND behavior.
- `orient index`: persistent Rust content-token, path-token, and trigram posting index.
- `orient ensure-index` / `orient refresh-index`: single-repo index bootstrap and incremental refresh that reuse unchanged file metadata/terms, detect same-content renames, and refresh changed files.
- `orient indexed-search`: indexed query path.
- `orient discover-repos`: bounded local repo discovery for broad workspaces and repeated worktree layouts, with git checkout boundaries by default and an explicit nested-manifest opt-in.
- `orient index-shards`, `orient ensure-shards`, `orient refresh-shards`, `orient search-shards`, `orient read-shard-range`, and `orient read-shard-ranges`: local multi-repo shard manifest with one versioned index file per repo, optional discovery from workspace roots, one-step build-or-refresh bootstrap, incremental shard refresh, bounded parallel direct shard search, git topology metadata, and bounded range reads from prefixed shard paths.
- `orient bench-search` and `orient bench-shards`: built-in p50/p95/max latency reporting for fallback, indexed, direct shard, and warm cached shard search, with `--fail-p95-ms`, `--write-baseline`, and `--baseline` for regression gates.
- JSON-lines tools: `tool_manifest`, `daemon_status`, `warm_index`, `ensure_index`, `refresh_index`, `index_status`, `warm_shards`, `discover_repos`, `search_code`, `search_batch`, `indexed_search_code`, `indexed_search_batch`, `indexed_query_plan`, `indexed_repo_map`, `read_index_range`, `read_index_ranges`, `find_index_symbol`, `shard_repo_map`, `find_shard_symbol`, `related_index_files`, `related_index_symbols`, `related_shard_files`, `related_shard_symbols`, `index_shards`, `ensure_shards`, `refresh_shards`, `shard_status`, `search_shards`, `search_shards_batch`, `shard_query_plan`, `read_shard_range`, `read_shard_ranges`, `repo_map`, `read_range`, `read_ranges`, and `related_symbols`.
- Local TCP daemon/client mode for sharing one warm JSON-lines runtime across many local agents working in the same repeated worktree layout, with startup prewarming via `--index` and `--index-dir`, cached shard manifests, compact daemon status details for warmed repos/aliases/git topology, cached single-index bootstrap via `ensure_index`, cached single-index refresh via `refresh_index`, cached parallel shard query plans, cached shard range/related-context followups, single-flight cold index loads, bounded parallel fanout for broad cached shard searches, `bench-shards --cached` parity with the warm runtime path, and no global search lock around cached index requests.
- `ensure_shards` JSON-lines bootstrap for shared daemons: build missing shard directories, refresh existing shard directories, prune missing repo roots, add newly discovered repo shards to existing shard directories, clear stale cache entries, and warm every shard index before agent traffic arrives.
- `index_status` and `shard_status` report live-file freshness versus persisted indexes, including added, changed, and deleted paths; indexed and shard searches can opt into `refresh_if_stale` for a one-call refresh-before-search path.
- CLI tools: `ensure-index`, `index-status`, `repo-map`, `index-map`, `index-plan`, `shard-status`, `shard-plan`, `shard-map`, `bench-shards`, `read-range`, `read-ranges`, `read-index-range`, `read-index-ranges`, `read-shard-ranges`, `index-symbol`, `shard-symbol`, `related-index`, `related-index-symbols`, `related-shard`, `related-shard-symbols`, and `related-symbols`, so agents can inspect freshness, entrypoints/tests/top symbols, debug indexed query planning, open bounded file context, benchmark shard search, and jump to nearby definitions after a search hit.
- `orient tool-manifest`: emits descriptions, compatibility required/optional argument names, typed argument metadata, defaults, enums, and JSON-schema-like input schemas for JSON-lines wrappers.
- Search snippet modes: `short`, `medium`, `block`, and `symbol`.
- Search results include structured `line_range` metadata, exact `match_lines` from indexed token-to-line tables when available, and a ready-to-pass `read_range` hint, allowing direct batch range reads and jump-to-line follow-up calls.
- Search requests can attach bounded line-numbered `context` ranges with `context_lines` / `--context-lines`, letting agents search and inspect edit context in one fallback, indexed, or shard round trip.
- Persistent indexes store bounded source snapshots, so indexed snippets, `read-index-range`, `read-index-ranges`, and shard range reads can return context from the saved index even when the live workspace file is unavailable.
- Path, file, repo, extension, language, and symbol filters match case-insensitively across fallback, indexed, and shard search surfaces.
- Filter-only discovery queries like `file:Cargo.toml` or `lang:rust test:true` work across fallback, indexed, shard, CLI, and JSON-lines search surfaces; indexed explain output reports them as `filter_scan`.
- JSON-lines search tools accept structured `exclude_*` filters as strings or arrays, so wrappers can express negative filters without query-string rewriting.
- Optional structured ranking explanations with path/content/term-frequency/symbol signals.
- Indexed explain mode includes query-plan metadata: planner strategy, normalized tokens, exact phrases, trigrams, rarest planned posting lists, broad-query candidate caps, missing postings, candidate counts through planning, file-filtering, phrase/scoring, and final-match stages, plus structured zero-hit repair hints; `index-plan` / `indexed_query_plan` and parallel `shard-plan` / `shard_query_plan` expose the same diagnostics for zero-result searches.
- Indexed search plans candidates from the rarest content/path token postings, falling back to rare trigram postings for substring queries, and caps broad candidate scoring after a cheap rank-aware prefilter.
- Indexed files persist line-offset and token-to-line tables for bounded snippet rendering and exact match-line metadata.
- Result de-duping and grouping for repeated worktree copies using normalized path suffixes and snippet signatures, with compact duplicate metadata on the kept result.
- Exact symbol definition boosting in both fallback and indexed search.
- Direct symbol lookup and related-context lookup from live and persistent indexes, including test-to-source stem matching for common `_test`, `test_`, `.test`, and `.spec` naming, so agent wrappers can jump to definitions and nearby tests/files without rebuilding a repo index.
- Direct symbol lookup across local shard directories, returning repo-prefixed paths that can be passed to `read-shard-range`.
- Bounded workspace discovery finds git or manifest-backed repo roots while skipping dependency/build directories, so agents can build shard directories from layouts like `Documents/Projects`, `~/repos`, and `.codex-worktrees` without manual repo lists. It prioritizes visible canonical repos before dated split, temp, and worktree folders when limits are small, treats git checkouts as traversal boundaries by default, accepts `nested_manifests` / `--nested-manifests` when an agent really wants package-level subprojects as separate shard candidates, and supports `family_limit` / `--family-limit` to cap selected checkouts per repeated git family while still reporting full family counts plus `candidates_found`. `index-shards` and `ensure-shards` include compact discovery summaries in their JSON output, and accept repeated discovery roots so one daemon can warm the canonical repos and active worktrees together.
- Repo-map orientation from live repos, persistent indexes, and shard directories, so agents can inspect entrypoints, manifests, tests, symbols, compact related-file/symbol hints, important files, and structured command hints without rebuilding a separate live repo index.
- Command hints are manifest-aware, include command kind/source provenance, and parse common `package.json` scripts while respecting package-manager lockfiles.
- Shard manifests record aliases for nested repo-looking child directories, so broad dated worktree shards can still answer stable filters like `repo:maestro` and scope results to the matching child path.
- Shard manifests record bounded git metadata for each shard, including origin, branch, clone/worktree kind, and common git dir when available. Shard repo filters and shard maps can use this topology, so agents can target an active branch or origin without knowing the exact checkout path.
- `daemon_status`, `warm_index`, `warm_shards`, and `serve-tcp --index-dir` expose compact warmed-index and warmed-shard details, so parallel local agents can confirm they are sharing the intended repo/branch shard set without session analytics.
- Alias-scoped shard search, symbol lookup, and repo maps emit stable alias-prefixed paths, so search hits like `maestro/src/foo.rs` can be opened without knowing the enclosing worktree shard name.
- Shard related-file and related-symbol tools accept alias-prefixed search-hit paths and keep returned context inside the same alias scope.
- Batch read tools open several repo, index, or shard result paths in one request, reducing JSON-lines round trips after a multi-result search; CLI batch reads also accept repeatable `--range path:start:lines` specs for search hits with different line windows.
- Shard refresh recomputes nested repo aliases, so newly added child repos become filterable after `refresh-shards`.
- `read-shard-range` resolves alias-prefixed paths, so agents can read `maestro/src/foo.rs` even when `maestro` lives inside a broader dated worktree shard.

Measured on this machine:

- Wide tree fallback: `/Users/jonathanhaas/Documents/Projects`, common top-10 literal/token queries at about `26-54ms` p95 after warmup across the latest sampled release run.
- Local repo fallback: query `indexed search symbol filters`, top 10 at about `12.5ms` p95 after warmup.
- Hot-path fallback has a `250ms` wall-clock timeout plus match caps; if the timeout fires it returns partial results instead of blocking the agent.
- Local repo index build: about `0.25s`.
- Local repo refresh after build: reuses unchanged files, reuses same-content renames by retargeting path-derived postings, and rebuilds postings from per-file term lists.
- Local repo indexed search: query `indexed search symbol filters`, top 10 at about `0.96ms` p95 after warmup.
- Local single-shard search: query `repo:agent-jsonl-explorer indexed search symbol filters`, top 10 at about `3.43ms` p95 after warmup, or about `1.01ms` p95 through the warm cached runtime path.
- Real local layout discovery: `/Users/jonathanhaas/Documents/Projects` now resolves to 409 git or manifest-backed repo roots at `max-depth 4` after scanning 508 directories, with the hottest repeated families being `maestro-internal` at 82 checkouts, `deploy` at 67, `platform` at 45, `browser-use-rs` at 30, and `maestro` at 23. `/Users/jonathanhaas/repos` resolves to 72 repo roots after scanning 106 directories. Before git-boundary discovery, the same broad tree could hit a 2,000-candidate cap by walking every nested package manifest.
- With `--family-limit 1`, the same `Documents/Projects` root selects 109 repo representatives from 409 candidates while preserving full family counts; `~/repos` selects 49 representatives from 72 candidates.

## Exit Conditions

High-performance definition:

- Wide-tree hot path returns useful top-10 results from `/Users/jonathanhaas/Documents/Projects` in `<=300ms` p95 for common literal/token queries.
- Repo-local searches return `<=100ms` p95 after warmup.
- Shard search has a first-class benchmark gate via `orient bench-shards --fail-p95-ms`.
- Indexed search beats fallback search on repeated repo-local queries.
- No multi-second hangs: candidate collection has bounded match caps and a hard wall-clock timeout.
- Top results avoid obvious duplicate spam from repeated worktrees.

Search quality definition:

- Query support covers literals, separator-normalized exact quoted phrases, multi-token AND semantics, path filters, extension/language filters, and exact-symbol boosts.
- Snippets include line numbers, exact match-line metadata, and enough context for an agent to decide whether to read/edit.
- Query-plan diagnostics explain zero-hit searches by separating missing postings from filter rejections, exact-phrase/scoring rejections, and final AND/symbol rejections, with structured repair hints an agent can retry.
- Search surfaces can optionally attach bounded read-range context to each hit when an agent wants fewer follow-up calls.
- Explain mode returns structured ranking signals when an agent needs to compare close results.
- CLI and JSON-lines server expose the same search capabilities, including multi-repo shard search.

Engineering definition:

- Persistent index has a versioned on-disk format.
- Persistent index stores separate content-token, path-token, and trigram postings.
- Multi-repo shard directories store a manifest plus one versioned index per repo, and can refresh those indexes incrementally.
- Persistent indexed files include line-offset tables, token-to-line tables, and bounded source snapshots for snippet and range retrieval.
- Incremental refresh covers add/edit/delete and same-content rename detection.
- Tests cover fallback search, indexed search, shard search/read tools, cross-surface golden retrieval, incremental refresh, filters, query parser stress cases, ranking explanations, duplicate suppression, JSON-lines server calls, corrupt index errors, path safety including symlink escapes, snippet modes, and a guarded `rg` differential check.
- Every release claim is backed by `cargo fmt --check`, `cargo test`, `cargo build --release`, and `orient bench-search` or equivalent timed searches, with saved baselines available for local or CI regression checks.

## Architecture Direction

Near term:

- Keep `rg` as the brutally fast no-index baseline.
- Add more query filters and aliases after the current path/language/extension/require-all surface.
- Add CI wiring around saved benchmark baselines once a stable runner is available.

Zoekt-inspired indexed mode:

- Move from token postings to trigram postings for substring and regex-like queries.
- Store the index in versioned shards, one shard per repo or repo slice.
- Use mmap-friendly binary layout rather than JSON once the schema stabilizes.
- Store compact file metadata, path postings, content postings, and richer line/term offset tables.
- Keep snapshot-backed snippet and range reads compact while exploring richer line/term offset tables.
- Add query planning: choose the rarest required posting lists first, then verify candidates.

Agent-specific differences from Zoekt:

- Return compact JSON objects built for tool use, not web UI rendering.
- Prefer de-duped, high-diversity top results over exhaustive match lists.
- Keep ranking local and deterministic; do not add session analytics or telemetry.
- Treat failed searches as an offline test-corpus signal when the user explicitly captures examples.
