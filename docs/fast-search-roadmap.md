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
- Agent-oriented query language for `file:`, `path:`, `lang:`, `ext:`, `symbol:`, `repo:`, `test:`, separator-normalized exact quoted phrases, negative filters, and default multi-term AND behavior.
- `orient index`: persistent Rust content-token, path-token, and trigram posting index.
- `orient refresh-index`: incremental refresh that reuses unchanged file metadata/terms, detects same-content renames, and refreshes changed files.
- `orient indexed-search`: indexed query path.
- `orient discover-repos`: bounded local repo discovery for broad workspaces and repeated worktree layouts.
- `orient index-shards`, `orient ensure-shards`, `orient refresh-shards`, `orient search-shards`, `orient read-shard-range`, and `orient read-shard-ranges`: local multi-repo shard manifest with one versioned index file per repo, optional discovery from workspace roots, one-step build-or-refresh bootstrap, incremental shard refresh, and bounded range reads from prefixed shard paths.
- `orient bench-search`: built-in p50/p95/max latency reporting for fallback and indexed search, with `--fail-p95-ms`, `--write-baseline`, and `--baseline` for regression gates.
- JSON-lines tools: `tool_manifest`, `daemon_status`, `warm_index`, `warm_shards`, `discover_repos`, `search_code`, `indexed_search_code`, `indexed_repo_map`, `read_index_range`, `read_index_ranges`, `find_index_symbol`, `shard_repo_map`, `find_shard_symbol`, `related_index_files`, `related_index_symbols`, `related_shard_files`, `related_shard_symbols`, `index_shards`, `ensure_shards`, `refresh_shards`, `search_shards`, `read_shard_range`, `read_shard_ranges`, `repo_map`, `read_range`, `read_ranges`, and `related_symbols`.
- Local TCP daemon/client mode for sharing one warm JSON-lines runtime across many local agents working in the same repeated worktree layout, with startup prewarming via `--index` and `--index-dir`, cached shard manifests, cached shard range/related-context followups, single-flight cold index loads, bounded parallel fanout for broad cached shard searches, and no global search lock around cached index requests.
- `ensure_shards` JSON-lines bootstrap for shared daemons: build missing shard directories, refresh existing shard directories, clear stale cache entries, and warm every shard index before agent traffic arrives.
- CLI tools: `repo-map`, `index-map`, `shard-map`, `read-range`, `read-ranges`, `read-index-range`, `read-index-ranges`, `read-shard-ranges`, `index-symbol`, `shard-symbol`, `related-index`, `related-index-symbols`, `related-shard`, `related-shard-symbols`, and `related-symbols`, so agents can inspect entrypoints/tests/top symbols, open bounded file context, and jump to nearby definitions after a search hit.
- `orient tool-manifest`: emits descriptions, compatibility required/optional argument names, typed argument metadata, defaults, enums, and JSON-schema-like input schemas for JSON-lines wrappers.
- Search snippet modes: `short`, `medium`, `block`, and `symbol`.
- Search results include structured `line_range` metadata derived from numbered snippets plus exact `match_lines` from indexed token-to-line tables when available, allowing direct read-range and jump-to-line follow-up calls.
- Search requests can attach bounded line-numbered `context` ranges with `context_lines` / `--context-lines`, letting agents search and inspect edit context in one fallback, indexed, or shard round trip.
- Persistent indexes store bounded source snapshots, so indexed snippets, `read-index-range`, `read-index-ranges`, and shard range reads can return context from the saved index even when the live workspace file is unavailable.
- Path, file, repo, extension, language, and symbol filters match case-insensitively across fallback, indexed, and shard search surfaces.
- JSON-lines search tools accept structured `exclude_*` filters as strings or arrays, so wrappers can express negative filters without query-string rewriting.
- Optional structured ranking explanations with path/content/term-frequency/symbol signals.
- Indexed explain mode includes query-plan metadata: planner strategy, normalized tokens, exact phrases, trigrams, rarest planned posting lists, and candidate count.
- Indexed search plans candidates from the rarest content/path token postings, falling back to rare trigram postings for substring queries.
- Indexed files persist line-offset and token-to-line tables for bounded snippet rendering and exact match-line metadata.
- Result de-duping and grouping for repeated worktree copies using normalized path suffixes and snippet signatures, with compact duplicate metadata on the kept result.
- Exact symbol definition boosting in both fallback and indexed search.
- Direct symbol lookup and related-context lookup from persistent indexes, so agent wrappers can jump to definitions and nearby tests/files without rebuilding a repo index.
- Direct symbol lookup across local shard directories, returning repo-prefixed paths that can be passed to `read-shard-range`.
- Bounded workspace discovery finds git or manifest-backed repo roots while skipping dependency/build directories, so agents can build shard directories from layouts like `Documents/Projects`, `~/repos`, and `.codex-worktrees` without manual repo lists. It prioritizes visible canonical repos before dated split, temp, and worktree folders when limits are small, and `index-shards` accepts repeated discovery roots so one daemon can warm the canonical repos and active worktrees together.
- Repo-map orientation from persistent indexes and shard directories, so agents can inspect entrypoints, manifests, tests, symbols, important files, and command hints without rebuilding a separate live repo index.
- Command hints are manifest-aware and parse common `package.json` scripts while respecting package-manager lockfiles.
- Shard manifests record aliases for nested repo-looking child directories, so broad dated worktree shards can still answer stable filters like `repo:maestro` and scope results to the matching child path.
- Alias-scoped shard search, symbol lookup, and repo maps emit stable alias-prefixed paths, so search hits like `maestro/src/foo.rs` can be opened without knowing the enclosing worktree shard name.
- Shard related-file and related-symbol tools accept alias-prefixed search-hit paths and keep returned context inside the same alias scope.
- Batch read tools open several repo, index, or shard result paths in one request, reducing JSON-lines round trips after a multi-result search.
- Shard refresh recomputes nested repo aliases, so newly added child repos become filterable after `refresh-shards`.
- `read-shard-range` resolves alias-prefixed paths, so agents can read `maestro/src/foo.rs` even when `maestro` lives inside a broader dated worktree shard.

Measured on this machine:

- Wide tree fallback: `/Users/jonathanhaas/Documents/Projects`, common top-10 literal/token queries at `20-37ms` p95 after warmup across the sampled runs, with a `26ms` p95 outlier on `session token auth` in the latest release run.
- Local repo fallback: query `indexed search symbol filters`, top 10 at about `12.5ms` p95 after warmup.
- Hot-path fallback has a `250ms` wall-clock timeout plus match caps; if the timeout fires it returns partial results instead of blocking the agent.
- Local repo index build: about `0.25s`.
- Local repo refresh after build: reuses unchanged files, reuses same-content renames by retargeting path-derived postings, and rebuilds postings from per-file term lists.
- Local repo indexed search: query `indexed search symbol filters`, top 10 at about `0.86ms` p95 after warmup.

## Exit Conditions

High-performance definition:

- Wide-tree hot path returns useful top-10 results from `/Users/jonathanhaas/Documents/Projects` in `<=300ms` p95 for common literal/token queries.
- Repo-local searches return `<=100ms` p95 after warmup.
- Indexed search beats fallback search on repeated repo-local queries.
- No multi-second hangs: candidate collection has bounded match caps and a hard wall-clock timeout.
- Top results avoid obvious duplicate spam from repeated worktrees.

Search quality definition:

- Query support covers literals, separator-normalized exact quoted phrases, multi-token AND semantics, path filters, extension/language filters, and exact-symbol boosts.
- Snippets include line numbers, exact match-line metadata, and enough context for an agent to decide whether to read/edit.
- Search surfaces can optionally attach bounded read-range context to each hit when an agent wants fewer follow-up calls.
- Explain mode returns structured ranking signals when an agent needs to compare close results.
- CLI and JSON-lines server expose the same search capabilities, including multi-repo shard search.

Engineering definition:

- Persistent index has a versioned on-disk format.
- Persistent index stores separate content-token, path-token, and trigram postings.
- Multi-repo shard directories store a manifest plus one versioned index per repo, and can refresh those indexes incrementally.
- Persistent indexed files include line-offset tables, token-to-line tables, and bounded source snapshots for snippet and range retrieval.
- Incremental refresh covers add/edit/delete and same-content rename detection.
- Tests cover fallback search, indexed search, shard search/read tools, incremental refresh, filters, query parser stress cases, ranking explanations, duplicate suppression, JSON-lines server calls, corrupt index errors, path safety including symlink escapes, snippet modes, and a guarded `rg` differential check.
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
